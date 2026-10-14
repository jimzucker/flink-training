# Example records

One record off each topic, read by the harness after the completeness run of build `a2d319dcf0390a80`.
Copied from the topic, not typed. The README links here so the formats cannot go stale.

## temperature-readings-small

The input, `temperature-readings`; read off `temperature-readings-small`, the completeness copy, which the same generator wrote.

Key:

```
loc-07
```

Value:

```
{"sensorId":"sensor-0070","location":"loc-07","readingTime":"2026-01-01T00:04:12Z","temperatureC":36.6}
```

## hourly-average-by-location

An output of the pipeline's own (`topicsAlsoWritten`).

Key:

```
loc-61
```

Value:

```
{"location":"loc-61","hourStart":"2026-01-01T00:00:00Z","totalC":385337.2,"count":14040}
```
