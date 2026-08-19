# Step 02 evidence

## Reproducibility on the wire

Two 10-second runs of the same seed, consumed back from Kafka and sorted:

```
run 1 sha256: 213d847f804ed9d0a057471c87da453d0ae6cea0e06ca7824e13002b43205d5c
run 2 sha256: 213d847f804ed9d0a057471c87da453d0ae6cea0e06ca7824e13002b43205d5c
```

## First record of the demo sequence

```json
{"allocations":[{"account":"ACC1","quantity":100,"subAccount":"SUB1"},{"account":"ACC2","quantity":100,"subAccount":"SUB1"},{"account":"ACC3","quantity":100,"subAccount":"SUB1"},{"account":"ACC4","quantity":100,"subAccount":"SUB1"}],"eventTime":1700000000000,"quantity":400,"side":"SELL","symbol":"GOOG","tradeId":"T000000000"}
```
