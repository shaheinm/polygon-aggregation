package main

import (
	"fmt"
	"time"
)

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

// Aggregation occurs every 30 seconds
func aggregation(ticker *time.Ticker, msgs []cryptoTrade) <-chan string {
	done := make(chan string)
	data := make(map[int64]aggregate)
	var state aggregate
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
			}
		}
	}()

	return done
}
