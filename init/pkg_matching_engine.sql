CREATE OR REPLACE PACKAGE pkg_matching_engine IS
  PROCEDURE match_orders_for_symbol(p_symbol IN VARCHAR2);
END pkg_matching_engine;
/

CREATE OR REPLACE PACKAGE BODY pkg_matching_engine IS
  PROCEDURE match_orders_for_symbol(p_symbol IN VARCHAR2) IS
    CURSOR c_buys IS
      SELECT order_id, customer_id, quantity, filled_quantity, price
      FROM orders
      WHERE symbol = p_symbol AND side = 'BUY' AND status IN ('PENDING','OPEN')
      ORDER BY price DESC, created_at;
    CURSOR c_sells IS
      SELECT order_id, customer_id, quantity, filled_quantity, price
      FROM orders
      WHERE symbol = p_symbol AND side = 'SELL' AND status IN ('PENDING','OPEN')
      ORDER BY price ASC, created_at;

    TYPE t_order IS RECORD (order_id NUMBER, customer_id NUMBER, quantity NUMBER, filled_quantity NUMBER, price NUMBER);
    v_buy t_order;
    v_sell t_order;
    v_trade_qty NUMBER;
    v_trade_price NUMBER;
    v_trade_id NUMBER;
  BEGIN
    OPEN c_buys;
    LOOP
      FETCH c_buys INTO v_buy;
      EXIT WHEN c_buys%NOTFOUND;
      OPEN c_sells;
      LOOP
        FETCH c_sells INTO v_sell;
        EXIT WHEN c_sells%NOTFOUND;
        IF v_buy.price >= v_sell.price THEN
          v_trade_qty := LEAST(v_buy.quantity - NVL(v_buy.filled_quantity,0), v_sell.quantity - NVL(v_sell.filled_quantity,0));
          v_trade_price := v_sell.price;
          IF v_trade_qty > 0 THEN
            v_trade_id := seq_trade.NEXTVAL;
            INSERT INTO trades(trade_id, buy_order_id, sell_order_id, buyer_id, seller_id, symbol, quantity, trade_price) VALUES (v_trade_id, v_buy.order_id, v_sell.order_id, v_buy.customer_id, v_sell.customer_id, p_symbol, v_trade_qty, v_trade_price);
            UPDATE orders SET filled_quantity = NVL(filled_quantity,0) + v_trade_qty,
                status = CASE WHEN NVL(filled_quantity,0) + v_trade_qty >= quantity THEN 'FILLED' ELSE 'PARTIAL' END
            WHERE order_id IN (v_buy.order_id, v_sell.order_id);
            MERGE INTO holdings h USING (SELECT v_buy.customer_id as cid, p_symbol sym, v_trade_qty as q, v_trade_price as pr FROM dual) src ON (h.customer_id = src.cid AND h.symbol = src.sym)
            WHEN MATCHED THEN UPDATE SET h.quantity = h.quantity + src.q, h.average_cost = ((NVL(h.quantity,0)*NVL(h.average_cost,0)) + (src.q * src.pr))/(NVL(h.quantity,0) + src.q)
            WHEN NOT MATCHED THEN INSERT (customer_id, symbol, quantity, average_cost) VALUES (src.cid, src.sym, src.q, src.pr);
            UPDATE holdings SET quantity = quantity - v_trade_qty WHERE customer_id = v_sell.customer_id AND symbol = p_symbol;
            INSERT INTO outbox_events(event_id, event_type, payload) VALUES (seq_event.NEXTVAL, 'TRADE_EXECUTED', '{"trade_id":'||v_trade_id||',"symbol":"'||p_symbol||'","qty":'||v_trade_qty||',"price":'||v_trade_price||'}');
            COMMIT;
          END IF;
        END IF;
      END LOOP;
      CLOSE c_sells;
    END LOOP;
    CLOSE c_buys;
  EXCEPTION WHEN OTHERS THEN
    ROLLBACK;
    INSERT INTO audit_errors(err_id, component, message) VALUES (seq_event.NEXTVAL, 'pkg_matching_engine', SQLERRM);
    COMMIT;
    RAISE;
  END;
END pkg_matching_engine;
/
