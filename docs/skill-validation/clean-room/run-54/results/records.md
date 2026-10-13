# Example records

One record off each topic, read by the harness after the completeness run of build `d7e14e7505068416`.
Copied from the topic, not typed. The README links here so the formats cannot go stale.

## orders-small

The input, `orders`; read off `orders-small`, the completeness copy, which the same generator wrote.

Key:

```
null
```

Value:

```
{"id":0,"symbol":"SYM1836","side":"BUY","quantity":1547,"allocations":[{"account":"ACC1","subAccount":"SUB1","quantity":151},{"account":"ACC1","subAccount":"SUB2","quantity":542},{"account":"ACC2","subAccount":"SUB1","quantity":363},{"account":"ACC2","subAccount":"SUB2","quantity":491}]}
```

## positions-by-symbol

An output the harness counts (`topics.out`).

Key:

```
SYM1836
```

Value:

```
{"symbol":"SYM1836","position":1547,"n":1}
```

## positions-by-account

An output the harness counts (`topics.out`).

Key:

```
ACC2/SUB2/SYM1836
```

Value:

```
{"account":"ACC2","subAccount":"SUB2","symbol":"SYM1836","position":491,"n":1}
```

## market-values-by-symbol

An output of the pipeline's own (`topicsAlsoWritten`).

Key:

```
SYM2490
```

Value:

```
{"symbol":"SYM2490","position":-2679,"priceCents":49351,"priceTs":1760000002000,"marketValueCents":-132211329,"n":7}
```

## market-values-by-account

An output of the pipeline's own (`topicsAlsoWritten`).

Key:

```
ACC1/SUB2/SYM1546
```

Value:

```
{"account":"ACC1","subAccount":"SUB2","symbol":"SYM1546","position":681,"priceCents":55885,"priceTs":1760000002000,"marketValueCents":38057685,"n":3}
```

## prices

A second input (`design.inputs`).

Key:

```
SYM0000
```

Value:

```
{"symbol":"SYM0000","ts":1760000000000,"priceCents":8802}
```
