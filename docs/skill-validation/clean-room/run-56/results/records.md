# Example records

One record off each topic, read by the harness after the completeness run of build `e35f29848e13cb4f`.
Copied from the topic, not typed. The README links here so the formats cannot go stale.

## orders-small

The input, `orders`; read off `orders-small`, the completeness copy, which the same generator wrote.

Key:

```
SYM4072
```

Value:

```
{"id":0,"symbol":"SYM4072","qty":186,"ts":1760000000000,"allocations":[{"account":"ACC0","sub":"SUB0","qty":61},{"account":"ACC0","sub":"SUB1","qty":31},{"account":"ACC1","sub":"SUB0","qty":89},{"account":"ACC1","sub":"SUB1","qty":5}]}
```

## positions-by-symbol

An output the harness counts (`topics.out`).

Key:

```
SYM2064
```

Value:

```
{"symbol":"SYM2064","position":54,"n":1,"orderId":64}
```

## positions-by-account

An output the harness counts (`topics.out`).

Key:

```
ACC1/SUB1/SYM1472
```

Value:

```
{"account":"ACC1","sub":"SUB1","symbol":"SYM1472","position":-14,"n":1,"orderId":24}
```

## market-values-by-symbol

An output of the pipeline's own (`topicsAlsoWritten`).

Key:

```
SYM0504
```

Value:

```
{"symbol":"SYM0504","position":7926,"priceCents":20414,"marketValueCents":161801364,"priceTs":1700000180504,"n":29}
```

## market-values-by-account

An output of the pipeline's own (`topicsAlsoWritten`).

Key:

```
ACC1/SUB1/SYM1640
```

Value:

```
{"account":"ACC1","sub":"SUB1","symbol":"SYM1640","position":120,"priceCents":5539,"marketValueCents":664680,"priceTs":1700000181640,"n":31}
```

## prices

A second input (`design.inputs`).

Key:

```
SYM2077
```

Value:

```
{"symbol":"SYM2077","ts":1700000002077,"priceCents":86832}
```
