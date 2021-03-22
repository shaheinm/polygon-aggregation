# Polygon.io 30s Aggregations

## What is it
A CLI that takes a crypto pair (i.e. BTC-USD, which is set by default), connects to the [Polygon.io Crypto Trades Websocket](https://polygon.io/docs/websockets/ws_crypto_XT_anchor), and outputs 30-second aggregates to stdout.

## How to use it
Set your Polygon.io API key in your environment:
```
export POLYGON_APIKEY=supersecretapikey
```
Build and run the application:
```
$ go build
$ ./polygon-aggregation --pair ETH-USD
[{"ev":"status","status":"connected","message":"Connected Successfully"}]
[{"ev":"status","status":"auth_success","message":"authenticated"}]
[{"ev":"status","status":"success","message":"subscribed to: XT.ETH-USD"}]
Trades in window: 69
19:53:59 - open: $1785.54, close: $1784.45, high: $1785.54, low: $1783.60, volume: 79.2175
Trades in window: 96
19:54:29 - open: $1784.41, close: $1784.77, high: $1785.00, low: $1783.60, volume: 126.3583
```