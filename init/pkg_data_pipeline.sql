CREATE OR REPLACE PACKAGE pkg_data_pipeline IS
  PROCEDURE load_historical_prices(p_load_date IN DATE);
END pkg_data_pipeline;
/

CREATE OR REPLACE PACKAGE BODY pkg_data_pipeline IS

  PROCEDURE load_historical_prices(p_load_date IN DATE) IS
    CURSOR c IS SELECT symbol, trade_date, open_price, high_price, low_price, close_price, adj_close, volume
                FROM stg_historical_price
                WHERE TRUNC(trade_date) = TRUNC(p_load_date);
    TYPE t_row_rec IS RECORD (
      symbol VARCHAR2(20), trade_date DATE, open_price NUMBER, high_price NUMBER, low_price NUMBER, close_price NUMBER, adj_close NUMBER, volume NUMBER
    );
    TYPE t_tab IS TABLE OF t_row_rec INDEX BY PLS_INTEGER;
    v_tab t_tab;
  BEGIN
    OPEN c;
    LOOP
      FETCH c BULK COLLECT INTO v_tab LIMIT 5000;
      EXIT WHEN v_tab.COUNT = 0;
      FOR i IN 1..v_tab.COUNT LOOP
        MERGE INTO fact_price tgt
        USING (SELECT v_tab(i).symbol sym, v_tab(i).trade_date dt FROM dual) src
        ON (tgt.symbol = src.sym AND tgt.price_date = src.dt)
        WHEN MATCHED THEN
          UPDATE SET open_price = v_tab(i).open_price,
                     high_price = v_tab(i).high_price,
                     low_price  = v_tab(i).low_price,
                     close_price = v_tab(i).close_price,
                     adj_close = v_tab(i).adj_close,
                     volume = v_tab(i).volume
        WHEN NOT MATCHED THEN
          INSERT (symbol, price_date, open_price, high_price, low_price, close_price, adj_close, volume)
          VALUES (v_tab(i).symbol, v_tab(i).trade_date, v_tab(i).open_price, v_tab(i).high_price, v_tab(i).low_price, v_tab(i).close_price, v_tab(i).adj_close, v_tab(i).volume);
      END LOOP;
      COMMIT;
    END LOOP;
    CLOSE c;
  EXCEPTION WHEN OTHERS THEN
    ROLLBACK;
    INSERT INTO audit_errors(err_id, component, message) VALUES (seq_event.NEXTVAL, 'pkg_data_pipeline', SQLERRM);
    COMMIT;
    RAISE;
  END;
END pkg_data_pipeline;
/
