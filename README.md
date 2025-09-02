# 📊 Stock Trading Data Pipeline

Hệ thống giao dịch chứng khoán tự động, xử lý 1000+ orders/giây, quản lý rủi ro real-time và tính toán settlement cuối ngày.

## 🚀 Features
- Xử lý khối lượng lớn lệnh mua/bán (Orders)
- Matching engine theo Price-Time Priority
- Risk management: VaR, volatility, limits
- Settlement tự động cuối ngày
- Real-time market data aggregation
- REST API cho order management

## 🏗️ Architecture
- Core Infrastructure: Oracle DB, Kafka, Docker
- ETL Pipeline: data ingestion → staging → tick replay → analytics → outbox events
- Trading Engines: matching_engine, risk_engine, settlement_worker
- API Layer: app.py (REST API)
- Monitoring & Analytics: Airflow DAGs, Grafana dashboards

## 📋 Workflows
- Order Flow: Investor → API → Orders → Kafka → Matching Engine → Trades
- Market Data Flow: Yahoo Finance → ETL → Tick Replay → Analytics Engine
- Risk Flow: Market Data → Risk Engine → Risk Metrics → Dashboard
- Settlement Flow: End of Day → Settlement Worker → P&L → Accounting

## 💡 Business Rules
- Order Matching (Price-Time Priority): if order_buy.price >= order_sell.price and order_buy.timestamp < order_sell.timestamp → execute_trade()
- Risk Limits: MAX_POSITION_SIZE=1M USD, MAX_DAILY_LOSS=50k USD, VOLATILITY_LIMIT=5%
- Settlement: Daily P&L → update account ledger

## 📊 Core Tables
Orders table: order_id, customer_id, symbol, side, quantity, price, status, created_ts  
Trades table: trade_id, buy_order_id, sell_order_id, symbol, quantity, price, trade_ts  
Risk metrics table: symbol, risk_ts, risk_score, volatility, var_95

## ⚡ Quick Start
1. docker-compose up -d  
2. docker-compose exec oracle-db sqlplus APP/app_pwd@XEPDB1 @/docker-entrypoint-initdb.d/init.sql  
3. docker-compose run --rm data-ingestion  
4. docker-compose run --rm load-staging  
5. curl -X POST http://localhost:8080/order -H "Content-Type: application/json" -d '{"customer_id":1001,"symbol":"AAPL","side":"BUY","quantity":100,"price":150.5}'  
6. docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

## 📊 Monitoring Endpoints
API Documentation: http://localhost:8080/docs  
Grafana Dashboard: http://localhost:3000  
Airflow UI: http://localhost:8081  
Kafka UI: http://localhost:9092

## 🛠️ Development Setup
Environment Variables: ORACLE_DSN=oracle-db:1521/XEPDB1, ORACLE_USER=APP, ORACLE_PWD=app_pwd, KAFKA_BOOTSTRAP_SERVERS=kafka:9092, TRADING_HOURS=09:30-16:00, RISK_LIMITS_ENABLED=true  
Running Tests: docker-compose run --rm api python -m pytest tests/unit/, docker-compose run --rm api python -m pytest tests/integration/, docker-compose run --rm k6 run /scripts/load_test.js

## 🔒 Security Features
SSL Encryption (Kafka, API), Role-based Access Control, Audit Logging, Rate Limiting, SQL Injection Protection

## 📚 API Examples
Place Order: POST /order {"customer_id":1001,"symbol":"AAPL","side":"BUY","quantity":100,"price":150.50}  
Get Portfolio: GET /portfolio/1001 (Authorization: Bearer <token>)  
Market Data: GET /market/aapl/history?period=1y, GET /market/aapl/ticks?from=2024-01-01

## 🤝 Contributing
Xem CONTRIBUTING.md để biết hướng dẫn đóng góp.

## 📞 Support
Issues: GitHub Issues  
Email: support@trading-platform.com  
Documentation: Wiki

## 📄 License
MIT License - xem LICENSE để biết chi tiết
