-- ================================
-- ETL: Load from STAGING to DIM & FACT
-- ================================

-- 1. Load DIM_SYMBOL
MERGE INTO dim_symbol d
USING (
    SELECT DISTINCT symbol FROM stg_historical_price
) s
ON (d.symbol = s.symbol)
WHEN NOT MATCHED THEN
    INSERT (symbol, company_name)
    VALUES (s.symbol, s.symbol || ' Inc.'); -- giả định tên công ty theo mã

-- 2. Load DIM_DATE
MERGE INTO dim_date d
USING (
    SELECT DISTINCT trade_date,
           EXTRACT(YEAR FROM trade_date) AS year,
           TO_NUMBER(TO_CHAR(trade_date, 'Q')) AS quarter,
           EXTRACT(MONTH FROM trade_date) AS month,
           EXTRACT(DAY FROM trade_date) AS day
    FROM stg_historical_price
) s
ON (d.trade_date = s.trade_date)
WHEN NOT MATCHED THEN
    INSERT (trade_date, year, quarter, month, day)
    VALUES (s.trade_date, s.year, s.quarter, s.month, s.day);

-- 3. Load FACT_PRICE_DAILY
INSERT INTO fact_price_daily (symbol_id, date_id, open_price, high_price, low_price, close_price, adj_close, volume)
SELECT 
    ds.symbol_id,
    dd.date_id,
    s.open_price,
    s.high_price,
    s.low_price,
    s.close_price,
    s.adj_close,
    s.volume
FROM stg_historical_price s
JOIN dim_symbol ds ON s.symbol = ds.symbol
JOIN dim_date dd ON s.trade_date = dd.trade_date
WHERE NOT EXISTS (
    SELECT 1 FROM fact_price_daily f
    WHERE f.symbol_id = ds.symbol_id AND f.date_id = dd.date_id
);

-- 4. Load FACT_PRICE_MONTHLY (aggregation)
INSERT INTO fact_price_monthly (symbol_id, year, month, avg_open, avg_close, avg_high, avg_low, total_volume)
SELECT 
    ds.symbol_id,
    EXTRACT(YEAR FROM s.trade_date) AS year,
    EXTRACT(MONTH FROM s.trade_date) AS month,
    AVG(s.open_price),
    AVG(s.close_price),
    AVG(s.high_price),
    AVG(s.low_price),
    SUM(s.volume)
FROM stg_historical_price s
JOIN dim_symbol ds ON s.symbol = ds.symbol
GROUP BY ds.symbol_id, EXTRACT(YEAR FROM s.trade_date), EXTRACT(MONTH FROM s.trade_date);
