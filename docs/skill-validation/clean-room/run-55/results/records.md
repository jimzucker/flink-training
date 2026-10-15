# Example records

One record off each topic, read by the harness after the completeness run of build `55e693b915fbe2a6`.
Copied from the topic, not typed. The README links here so the formats cannot go stale.

## orders-small

The input, `orders`; read off `orders-small`, the completeness copy, which the same generator wrote.

Key:

```
S0272
```

Value:

```
{"id":0,"sym":"S0272","qty":416,"ts":1760000000000,"alloc":[{"acct":"ACC1","sub":"SUB1","qty":-49},{"acct":"ACC1","sub":"SUB2","qty":352},{"acct":"ACC2","sub":"SUB1","qty":-370},{"acct":"ACC2","sub":"SUB2","qty":483}]}
```

## positions-by-symbol

An output the harness counts (`topics.out`).

Key:

```
S3328
```

Value:

```
{"sym":"S3328","pos":-993,"n":1}
```

## positions-by-account

An output the harness counts (`topics.out`).

Key:

```
ACC1/SUB2/S3328
```

Value:

```
{"acct":"ACC1","sub":"SUB2","sym":"S3328","pos":-357,"n":1}
```

## market-values-by-symbol

An output of the pipeline's own (`topicsAlsoWritten`).

Key:

```
S3184
```

Value:

```
{"sym":"S3184","pos":6965,"price":468264,"priceTs":1760000002000,"mv":3261458760,"n":63}
```

## market-values-by-account

An output of the pipeline's own (`topicsAlsoWritten`).

Key:

```
ACC1/SUB2/S3184
```

Value:

```
{"acct":"ACC1","sub":"SUB2","sym":"S3184","pos":1597,"price":468264,"priceTs":1760000002000,"mv":747817608,"n":63}
```

## prices

A second input (`design.inputs`).

Key:

```
S4093
```

Value:

```
{"sym":"S4093","ts":1760000002000,"cents":47625}
```
