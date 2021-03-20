/*
	Still TODO:
		- Implement concurrency pattern (https://blog.golang.org/pipelines)
		- Convert to CLI
		- Add tests
		- Clear map after hour or find a better storage solution than in code.
*/
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	websocket "github.com/gorilla/websocket"
)

// INTERVAL 30 second intervals
const INTERVAL = 3e10

type cryptoTrade struct {
	EventType  string  `json:"ev"`
	Symbol     string  `json:"pair"`
	Price      float64 `json:"p"`
	Timestamp  int64   `json:"t"`
	Size       float64 `json:"s"`
	Conditions []int   `json:"c"`
	TradeID    string  `json:"i"`
	ExchID     int     `json:"x"`
	PolyTime   int64   `json:"r"`
}

type aggregate struct {
	interval string
	open     float64
	cp       float64 // closing price for interval
	high     float64
	low      float64
	volume   float64
}

func prettyPrintAggregate(agg aggregate, trades int, update bool) {
	if update {
		fmt.Printf("Received trade from previous window, updated Aggregate:\n%s - open: $%.2f, close: $%.2f, high: $%.2f, low: $%.2f, volume: %.4f\n", agg.interval, agg.open, agg.cp, agg.high, agg.low, agg.volume)
		return
	}

	fmt.Printf("Trades in window: %d\n%s - open: $%.2f, close: $%.2f, high: $%.2f, low: $%.2f, volume: %.4f\n", trades, agg.interval, agg.open, agg.cp, agg.high, agg.low, agg.volume)
}

func handleUpdatedAggregate(aggMap map[int64]aggregate, trade cryptoTrade, tradeTime time.Time) {
	var updatedAggregate aggregate
	for k, v := range aggMap {
		aggInterval := time.Unix(0, k)
		if aggInterval.Before(tradeTime) {
			continue
		}

		updatedAggregate.interval = v.interval
		updatedAggregate.open = v.open
		updatedAggregate.cp = v.cp
		updatedAggregate.volume = v.volume + trade.Size

		if v.high < trade.Price {
			fmt.Printf("Trade price: %.2f, Previous High: %.2f\n", trade.Price, v.high)
			updatedAggregate.high = trade.Price
			fmt.Print("New High Price set:", updatedAggregate.high)
		} else {
			updatedAggregate.high = v.high
		}

		if v.low > trade.Price {
			fmt.Printf("Trade price: %.2f, Previous Low: %.2f\n", trade.Price, v.low)
			updatedAggregate.low = trade.Price
			fmt.Print("New Low Price set:", updatedAggregate.low)
		} else {
			updatedAggregate.low = v.low
		}

		v = updatedAggregate
		break
	}

	prettyPrintAggregate(updatedAggregate, 0, true)
}

func main() {
	// APIKEY to connect to polygon
	APIKEY := os.Getenv("POLYGON_APIKEY")

	// CHANNELS to subscribe to - TODO: convert to cmd line input
	CHANNELS := os.Getenv("POLYGON_WS_CHANNELS") // "XT.BTC-USD"

	var msgs []cryptoTrade
	var state aggregate
	data := make(map[int64]aggregate)
	var mutex = &sync.Mutex{}

	c, _, err := websocket.DefaultDialer.Dial("wss://socket.polygon.io/crypto", nil)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	ticker := time.NewTicker(30000 * time.Millisecond)
	defer ticker.Stop()
	done := make(chan bool)
	go func() {
		time.Sleep(61 * time.Second)
		done <- true
	}()

	// aggregate every 30 seconds
	go func() {
		for {
			select {
			case <-done:
				fmt.Println("Done. Here are all the aggregates:")
				for _, v := range data {
					prettyPrintAggregate(v, len(data), false)
				}
				return
			case t := <-ticker.C:
				// t is the last tick in 30 second interval
				cutoff := t.UnixNano() - INTERVAL
				// 1 hour before cutoff
				stale := cutoff - 3.6e12
				mutex.Lock()
				state.interval = t.Format("15:04:05")
				state.open = msgs[0].Price
				state.cp = msgs[len(msgs)-1].Price
				// Initial value
				state.low = msgs[0].Price
				for i := 0; i < len(msgs); i++ {
					timestamp := time.Unix(0, msgs[i].Timestamp*int64(time.Millisecond))
					// Trades that come through more than hour after execution are left alone
					if timestamp.Before(time.Unix(0, stale)) {
						continue
					} else if timestamp.Before(time.Unix(0, cutoff)) && timestamp.After(time.Unix(0, stale)) {
						// Trades less than hour after execution but before the current interval print updated aggregate
						fmt.Println("Late trade: ", timestamp)
						handleUpdatedAggregate(data, msgs[i], timestamp)
						continue
					}

					if msgs[i].Price > state.high {
						state.high = msgs[i].Price
					}

					if msgs[i].Price < state.low {
						state.low = msgs[i].Price
					}

					state.volume += msgs[i].Size
				}
				// Print interval results to stdout
				prettyPrintAggregate(state, len(msgs), false)
				// Clear data for next interval
				data[t.UnixNano()] = state
				state = aggregate{}
				msgs = []cryptoTrade{}
				mutex.Unlock()
			}
		}
	}()

	_ = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("{\"action\":\"auth\",\"params\":\"%s\"}", APIKEY)))
	_ = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("{\"action\":\"subscribe\",\"params\":\"%s\"}", CHANNELS)))

	// Buffered channel to account for bursts or spikes in data:
	chanMessages := make(chan []byte, 10000)

	// Read messages off the buffered queue:
	go func() {
		var msg []cryptoTrade
		for msgBytes := range chanMessages {
			json.Unmarshal(msgBytes, &msg)
			for i := 0; i < len(msg); i++ {
				if msg[i].EventType == "status" {
					fmt.Println(string(msgBytes))
					continue
				}
				mutex.Lock()
				msgs = append(msgs, msg[i])
				mutex.Unlock()
			}
		}
	}()

	// Push messages to channel
	for {
		_, msg, err := c.ReadMessage()
		if err != nil {
			panic(err)
		}
		chanMessages <- msg
	}

}
