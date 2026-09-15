#!/bin/bash
# Each stage alone, one thread, its own JVM, both order sizes, 3 repeats interleaved.
# --cpus 2 so JIT and GC threads do not steal from the timed thread.
S=$(cd "$(dirname "$0")" && pwd)
J=${JOB_DIR:?set JOB_DIR to a directory holding jobs.jar and generators.jar}
OUT=$S/stages.log
echo "start $(date -u +%FT%TZ)" > $OUT
for rep in 1 2 3; do
  for stage in fetch decode tokens skip parse symbol split ser deser; do
    for f in 0 32; do
      line=$(docker run --rm --cpus 2 -m 3g -v $S:/bench:ro -v $J:/job:ro --entrypoint java \
        flink:1.20.1-scala_2.12-java17 -Xms2g -Xmx2g \
        -cp "/bench/stageclasses:/job/jobs.jar:/job/generators.jar:/opt/flink/lib/*" \
        StageBench $f $stage 8 12 2>&1 | tail -3)
      echo "rep=$rep $line" >> $OUT
    done
  done
done
echo "DONE $(date -u +%FT%TZ)" >> $OUT
