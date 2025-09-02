CREATE OR REPLACE PACKAGE pkg_settlement IS
  PROCEDURE run_daily_settlement(p_settlement_date IN VARCHAR2);
END pkg_settlement;
/

CREATE OR REPLACE PACKAGE BODY pkg_settlement IS
  PROCEDURE run_daily_settlement(p_settlement_date IN VARCHAR2) IS
    v_date DATE := TO_DATE(p_settlement_date, 'YYYY-MM-DD');
  BEGIN
    MERGE INTO fact_trade_agg tgt
    USING (
      SELECT symbol, TRUNC(trade_time) trade_date, SUM(quantity) total_qty, SUM(quantity*trade_price) total_value
      FROM trades
      WHERE TRUNC(trade_time) = v_date
      GROUP BY symbol, TRUNC(trade_time)
    ) src
    ON (tgt.symbol = src.symbol AND tgt.trade_date = src.trade_date)
    WHEN MATCHED THEN UPDATE SET total_qty = src.total_qty, total_value = src.total_value
    WHEN NOT MATCHED THEN INSERT (symbol, trade_date, total_qty, total_value) VALUES (src.symbol, src.trade_date, src.total_qty, src.total_value);
    COMMIT;
  EXCEPTION WHEN OTHERS THEN
    ROLLBACK;
    INSERT INTO audit_errors(err_id, component, message) VALUES (seq_event.NEXTVAL, 'pkg_settlement', SQLERRM);
    COMMIT;
    RAISE;
  END;
END pkg_settlement;
/
