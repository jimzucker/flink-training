#!/bin/bash
# Per-order CPU work (both flatMaps + PositionUpdate serialization), no Kafka, no Flink runtime.
# Same image as the rig; --cpus = threads; same heap for every case; repeats interleaved.
S=$(cd "$(dirname "$0")" && pwd)
J=${JOB_DIR:?set JOB_DIR to a directory holding jobs.jar and generators.jar}
OUT=$S/matrix.log
echo "start $(date -u +%FT%TZ)" > $OUT
for rep in 1 2 3; do
  for f in 0 32; do
    for t in 1 2 4; do
      line=$(docker run --rm --cpus $t -m 3g -v $S:/bench:ro -v $J:/job:ro --entrypoint java \
        flink:1.20.1-scala_2.12-java17 -Xms2g -Xmx2g \
        -cp "/bench/classes:/job/jobs.jar:/job/generators.jar:/opt/flink/lib/*" \
        PayloadBench $f $t 10 20 full 2>&1 | tail -3)
      echo "rep=$rep $line" >> $OUT
    done
  done
done
echo "DONE $(date -u +%FT%TZ)" >> $OUT
