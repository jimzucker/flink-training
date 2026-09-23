# The six answers (from the user, verbatim)

**1. What goes into the pipeline, and what comes out?**

> A stream of temperature readings. Each reading carries a sensor id, a
> location, a date/time and a temperature in Celsius. One reading per sensor per
> second. The pipeline emits the average temperature for each location for every
> 1-hour bucket.
>
> The hour bucket is computed from **the timestamp field inside each reading**.
> The clock on the machine is never used for bucketing. Replaying the same input
> gives byte-identical output.

**2. Does one input produce more than one output?**

> No. The hourly average is the only thing published. Nothing is emitted per
> reading.

**3. What are the keys, and how many distinct ones?**

> 100 locations, 10 sensors each, so 1,000 sensors. The average is per
> **location**, so the key space is location: 100 distinct keys.

**4. What has to be exactly right?**

> Every hourly average equals the true average of that location's readings in
> that hour. No window missing, none counted twice, and the same answer on a
> replay. **Late readings are ignored** — a reading whose hour has already been
> published does not reopen it. The output carries the **total and the count**,
> not a rounded average, so there is no rounding to argue about.

**5. Which Flink API?** DataStream.

**6. Which Kafka?** Apache.
