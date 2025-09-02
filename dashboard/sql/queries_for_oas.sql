-- Top traded symbols (today)
SELECT symbol, SUM(quantity) as total_qty, SUM(quantity*trade_price) as total_value
FROM trades
WHERE TRUNC(trade_time) = TRUNC(SYSDATE)
GROUP BY symbol ORDER BY total_qty DESC;

-- Latest PnL per customer (example)
SELECT h.customer_id, h.symbol, h.quantity, h.average_cost,
  (SELECT close_price FROM fact_price fp WHERE fp.symbol=h.symbol AND fp.price_date=(SELECT MAX(price_date) FROM fact_price WHERE symbol=h.symbol)) current_price,
  h.quantity * ((SELECT close_price FROM fact_price fp WHERE fp.symbol=h.symbol AND fp.price_date=(SELECT MAX(price_date) FROM fact_price WHERE symbol=h.symbol)) - h.average_cost) unrealized_pnl
FROM holdings h WHERE h.customer_id = :customer_id;

-- Liquidity risk recent
SELECT symbol, risk_ts, risk_score FROM fact_symbol_risk WHERE risk_ts >= TRUNC(SYSDATE-1) ORDER BY risk_score DESC;
