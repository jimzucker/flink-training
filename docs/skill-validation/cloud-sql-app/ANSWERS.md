# Answers — the cloud SQL app

Written by Claude for the user's request of 2026-10-05 ("do both one after the other"): one app that
runs unchanged on the laptop and on Confluent Cloud, to run the harness's whole chain on the cloud.

1. **Input and output:** an order (id, symbol, account, quantity, price) becomes the same order with
   its notional value (quantity × price).
2. **Fan-out:** one output per input.
3. **Keys:** 4 accounts, 4,096 symbols; nothing is keyed in the job.
4. **What has to be exactly right:** every order once in the output — per account, the count and the
   total quantity equal the input's, on both platforms. Exactly-once checkpointing and an
   exactly-once sink.
5. **Audience:** engineers checking that the cloud harness measures the same app the laptop did.
6. **Where:** the laptop first (this file), then Confluent Cloud (pipeline-cloud.json).
7. **Claim:** the same SQL INSERT passes completeness on both platforms, and on Confluent Cloud the
   harness's chain measures it end to end.
8. **Axis:** laptop — one machine, more cores; cloud — CFU (one subtask per CFU, findings §8).
9. **API:** SQL.
