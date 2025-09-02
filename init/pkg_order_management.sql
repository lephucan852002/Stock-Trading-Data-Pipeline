CREATE OR REPLACE PACKAGE pkg_order_management IS
  PROCEDURE place_order(p_customer_id IN NUMBER, p_symbol IN VARCHAR2, p_side IN VARCHAR2, p_qty IN NUMBER, p_price IN NUMBER, p_order_id OUT NUMBER);
END pkg_order_management;
/

CREATE OR REPLACE PACKAGE BODY pkg_order_management IS
  PROCEDURE place_order(p_customer_id IN NUMBER, p_symbol IN VARCHAR2, p_side IN VARCHAR2, p_qty IN NUMBER, p_price IN NUMBER, p_order_id OUT NUMBER) IS
    v_order_id NUMBER;
    v_payload CLOB;
  BEGIN
    v_order_id := seq_order.NEXTVAL;
    INSERT INTO orders(order_id, customer_id, symbol, side, quantity, price, status, created_at) VALUES (v_order_id, p_customer_id, p_symbol, p_side, p_qty, p_price, 'PENDING', SYSTIMESTAMP);
    v_payload := '{"order_id":' || v_order_id || ',"customer_id":' || p_customer_id || ',"symbol":"' || p_symbol || '","side":"' || p_side || '","qty":' || p_qty || ',"price":' || p_price || '}';
    INSERT INTO outbox_events(event_id, event_type, payload) VALUES (seq_event.NEXTVAL, 'ORDER_CREATED', v_payload);
    COMMIT;
    p_order_id := v_order_id;
  EXCEPTION WHEN OTHERS THEN
    ROLLBACK;
    INSERT INTO audit_errors(err_id, component, message) VALUES (seq_event.NEXTVAL, 'pkg_order_management', SQLERRM);
    COMMIT;
    RAISE;
  END place_order;
END pkg_order_management;
/
