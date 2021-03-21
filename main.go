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

func main() {
	// APIKEY to connect to polygon
	APIKEY := os.Getenv("POLYGON_APIKEY")

	// CHANNELS to subscribe to - TODO: convert to cmd line input
	CHANNELS := os.Getenv("POLYGON_WS_CHANNELS") // "XT.BTC-USD"

	var msgs []cryptoTrade

	c, _, err := websocket.DefaultDialer.Dial("wss://socket.polygon.io/crypto", nil)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	ticker := time.NewTicker(30000 * time.Millisecond)
	defer ticker.Stop()

	// aggregate every 30 seconds
	out := aggregation(ticker, msgs)
	fmt.Print(out)

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
				msgs = append(msgs, msg[i])
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
