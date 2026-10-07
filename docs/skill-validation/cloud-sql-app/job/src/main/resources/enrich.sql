INSERT INTO enriched SELECT order_id, symbol, account, qty, price, qty * price AS notional FROM `{in}`
