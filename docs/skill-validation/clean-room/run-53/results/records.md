# Example records

One record off each topic, read by the harness after the completeness run of build `b0d1448e7640f0c3`.
Copied from the topic, not typed. The README links here so the formats cannot go stale.

## orders-small

The input, `orders`; read off `orders-small`, the completeness copy, which the same generator wrote.

Key:

```
null
```

Value:

```
{"id":0,"symbol":"SYM0796","qty":-459,"allocs":[{"account":"A1","sub":"S1","qty":-127},{"account":"A1","sub":"S2","qty":-157},{"account":"A2","sub":"S1","qty":-152},{"account":"A2","sub":"S2","qty":-23}]}
```

## positions-by-symbol

An output the harness counts (`topics.out`).

Key:

```
SYM2658
```

Value:

```
{"symbol":"SYM2658","position":164,"orders":1}
```

## positions-by-account

An output the harness counts (`topics.out`).

Key:

```
A2|S1|SYM2443
```

Value:

```
{"account":"A2","sub":"S1","symbol":"SYM2443","position":-189,"orders":1}
```

## market-values-by-symbol

An output of the pipeline's own (`topicsAlsoWritten`).

Key:

```
SYM2856
```

Value:

```
{"symbol":"SYM2856","position":-1389,"price":60643,"priceTs":1760065866446,"marketValue":-84233127,"orders":5}
```

## market-values-by-account

An output of the pipeline's own (`topicsAlsoWritten`).

Key:

```
A2|S2|SYM1482
```

Value:

```
{"account":"A2","sub":"S2","symbol":"SYM1482","position":-337,"price":87419,"priceTs":1760064180667,"marketValue":-29460203,"orders":9}
```

## prices

A second input (`design.inputs`).

Key:

```
SYM0000
```

Value:

```
{"symbol":"SYM0000","ts":1760071467500,"price":38983}
```
