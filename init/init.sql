-- Run as APP/app_pwd@XEPDB1 after Oracle ready
CREATE USER APP IDENTIFIED BY app_pwd;
GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE PROCEDURE, CREATE SEQUENCE TO APP;

CONNECT APP/app_pwd@XEPDB1;

-- sequences
CREATE SEQUENCE seq_order START WITH 1 NOCACHE;
CREATE SEQUENCE seq_trade START WITH 1 NOCACHE;
CREATE SEQUENCE seq_event START WITH 1 NOCACHE;

-- STAGING
CREATE TABLE stg_historical_price (
  symbol VARCHAR2(20),
  trade_date DATE,
  open_price NUMBER,
  high_price NUMBER,
  low_price NUMBER,
  close_price NUMBER,
  adj_close NUMBER,
  volume NUMBER,
  load_ts TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE stg_minute_bar (
  symbol VARCHAR2(20),
  minute_ts TIMESTAMP,
  open_price NUMBER, high_price NUMBER, low_price NUMBER, close_price NUMBER,
  volume NUMBER,
  load_ts TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE stg_corp_actions (
  symbol VARCHAR2(20), action_date DATE, action_type VARCHAR2(50), details CLOB, load_ts TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- OLTP
CREATE TABLE dim_customers (
  customer_id NUMBER PRIMARY KEY,
  customer_name VARCHAR2(200),
  cash_balance NUMBER(18,2),
  created_at TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE dim_security (
  symbol VARCHAR2(20) PRIMARY KEY,
  company_name VARCHAR2(200),
  sector VARCHAR2(100),
  industry VARCHAR2(100)
);

CREATE TABLE orders (
  order_id NUMBER PRIMARY KEY,
  customer_id NUMBER,
  symbol VARCHAR2(20),
  side VARCHAR2(4),
  quantity NUMBER,
  filled_quantity NUMBER DEFAULT 0,
  price NUMBER,
  status VARCHAR2(20),
  created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE trades (
  trade_id NUMBER PRIMARY KEY,
  buy_order_id NUMBER,
  sell_order_id NUMBER,
  buyer_id NUMBER,
  seller_id NUMBER,
  symbol VARCHAR2(20),
  quantity NUMBER,
  trade_price NUMBER,
  trade_time TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE holdings (
  customer_id NUMBER,
  symbol VARCHAR2(20),
  quantity NUMBER,
  average_cost NUMBER,
  PRIMARY KEY (customer_id, symbol)
);

-- DW / OLAP
CREATE TABLE fact_price (
  symbol VARCHAR2(20),
  price_date DATE,
  open_price NUMBER,
  high_price NUMBER,
  low_price NUMBER,
  close_price NUMBER,
  adj_close NUMBER,
  volume NUMBER,
  PRIMARY KEY(symbol, price_date)
);

CREATE TABLE fact_tick_agg (
  symbol VARCHAR2(20),
  minute_ts TIMESTAMP,
  open_price NUMBER, high_price NUMBER, low_price NUMBER, close_price NUMBER,
  volume NUMBER,
  tick_count NUMBER,
  PRIMARY KEY(symbol, minute_ts)
);

CREATE TABLE fact_trade_agg (
  symbol VARCHAR2(20),
  trade_date DATE,
  total_qty NUMBER,
  total_value NUMBER,
  PRIMARY KEY(symbol, trade_date)
);

CREATE TABLE fact_symbol_risk (
  symbol VARCHAR2(20),
  risk_ts TIMESTAMP,
  risk_score NUMBER,
  model_version VARCHAR2(50),
  PRIMARY KEY(symbol, risk_ts)
);

-- Outbox / support
CREATE TABLE outbox_events (
  event_id NUMBER PRIMARY KEY,
  event_type VARCHAR2(50),
  payload CLOB,
  created_ts TIMESTAMP DEFAULT SYSTIMESTAMP,
  dispatched CHAR(1) DEFAULT 'N'
);

CREATE TABLE audit_errors (
  err_id NUMBER PRIMARY KEY,
  component VARCHAR2(100),
  err_ts TIMESTAMP DEFAULT SYSTIMESTAMP,
  message CLOB
);

COMMIT;
