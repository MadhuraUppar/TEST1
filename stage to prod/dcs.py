import psycopg2
import sys
sys.path.append('C:/Users/madhura.uppar/Downloads/New folder/TEST1')
import mainsetvariable as mn

def copy_data_between_schemas(table_name):
    # Redshift connection parameters
    host = 'default-workgroup.115203216969.us-east-1.redshift-serverless.amazonaws.com'
    port = '5439'
    user = 'admin'
    password = 'Madhu123'
    dbname = 'dev'

    # Establish connection
    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
    cursor = conn.cursor()

    try:
        # Build the COPY command to move data between schemas
        copy_command = f"""
insert into devdw.daily_customer_summary (summary_date,
dw_customer_id,
order_count,
order_apd,
order_cost_amount,
order_mrp_amount,
products_ordered_qty,
products_items_qty,
cancelled_order_count,
cancelled_order_amount,
cancelled_order_apd,
shipped_order_count,
shipped_order_amount,
shipped_order_apd,
payment_apd,
payment_amount,
new_customer_apd,
new_customer_paid_apd,
dw_create_timestamp,
dw_update_timestamp,
etl_batch_no,
etl_batch_date)
SELECT d.Summary_date,
       d.dw_customer_id,
       MAX(d.order_count) AS order_count,
       MAX(d.order_opd) AS order_opd,
       MAX(d.order_cost_amount) AS order_cost_amt,
       MAX(d.order_mrp_amount) AS order_mrp_amount,
       MAX(d.products_ordered_qty) AS product_ord_qty,
       MAX(d.products_items_qty) AS prod_item_qty,
       MAX(d.cancelled_order_count) AS cancelled_order_count,
       MAX(d.cancelled_order_amount) AS cancelled_order_amount,
       MAX(d.cancelled_order_apd) AS Cancelled_order_apd,
       MAX(d.shipped_order_count) AS shipped_ord_count,
       MAX(d.shipped_order_amount) AS shipped_ord_amt,
       MAX(d.shipped_order_apd) AS shipped_ord_apd,
       MAX(d.payment_apd) AS payment_apd,
       MAX(d.payment_amount) AS payment_amt,
       MAX(d.new_customer_apd) AS new_cust_apd,
       0,
       current_timestamp,
       current_timestamp,
       {mn.etl_batch_no},
       cast('{mn.etl_batch_date}' as date)
FROM (SELECT o.orderDate AS Summary_date,
             o.dw_customer_id,
             COUNT(DISTINCT o.dw_order_id) AS order_count,
             1 AS order_opd,
             SUM(od.quantityOrdered*od.priceEach) AS order_cost_amount,
             SUM(od.quantityOrdered*p.MSRP) AS order_mrp_amount,
             COUNT(od.dw_product_id) AS products_ordered_qty,
             COUNT(DISTINCT p.productLine) AS products_items_qty,
             0 AS cancelled_order_count,
             0 AS cancelled_order_amount,
             0 AS cancelled_order_apd,
             0 AS shipped_order_count,
             0 AS shipped_order_amount,
             0 AS shipped_order_apd,
             0 AS payment_apd,
             0 AS payment_amount,
             0 AS new_customer_apd
      FROM devdw.orders o
        JOIN devdw.orderdetails od ON o.dw_order_id = od.dw_order_id
        JOIN devdw.products p ON p.dw_product_id = od.dw_product_id
      WHERE o.orderDate >= cast('{mn.etl_batch_date}' as date)
      GROUP BY o.orderDate,
               o.dw_customer_id
      UNION ALL
      SELECT o.cancelledDate AS Summary_date,
             o.dw_customer_id,
             0 AS order_count,
             0 AS order_opd,
             0 AS order_cost_amt,
             0 AS order_mrp_amount,
             0 AS product_ord_qty,
             0 AS prod_item_qty,
             COUNT(DISTINCT o.dw_order_id) AS cancelled_order_count,
             SUM(od.quantityOrdered*od.priceEach) AS cancelled_order_amount,
             1 AS Cancelled_order_apd,
             0 AS shipped_ord_count,
             0 AS shipped_ord_amt,
             0 AS shipped_ord_apd,
             0 AS payment_apd,
             0 AS payment_amt,
             0 AS new_cust_apd
      FROM devdw.orders o
        JOIN devdw.orderdetails od ON o.dw_order_id = od.dw_order_id
      WHERE o.cancelledDate >= cast('{mn.etl_batch_date}' as date)
      GROUP BY o.cancelledDate,
               o.dw_customer_id
      UNION ALL
      SELECT o.shippedDate AS Summary_date,
             o.dw_customer_id,
             0 AS order_count,
             0 AS order_opd,
             0 AS order_cost_amt,
             0 AS order_mrp_amount,
             0 AS product_ord_qty,
             0 AS prod_item_qty,
             0 AS cancelled_order_count,
             0 AS cancelled_order_amount,
             0 AS Cancelled_order_apd,
             COUNT(DISTINCT o.dw_order_id) AS shipped_order_count,
             SUM(od.quantityOrdered*od.priceEach) AS shipped_order_amount,
             1 AS shipped_ord_apd,
             0 AS payment_apd,
             0 AS payment_amt,
             0 AS new_cust_apd
      FROM devdw.orders o
        JOIN devdw.orderdetails od ON o.dw_order_id = od.dw_order_id
      WHERE o.shippedDate >= cast('{mn.etl_batch_date}' as date)
      GROUP BY o.shippedDate,
               o.dw_customer_id
      UNION ALL
      SELECT paymentDate AS Summary_date,
             dw_customer_id,
             0 AS order_count,
             0 AS order_opd,
             
             0 AS order_cost_amt,
             0 AS order_mrp_amount,
             0 AS product_ord_qty,
             0 AS prod_item_qty,
             0 AS cancelled_order_count,
             0 AS cancelled_order_amount,
             0 AS Cancelled_order_apd,
             0 AS shipped_ord_count,
             0 AS shipped_ord_amt,
             0 AS shipped_ord_apd,
             1 AS payment_apd,
             SUM(amount) AS payment_amount,
             0 AS new_cust_apd
      FROM devdw.payments
      WHERE paymentDate >= cast('{mn.etl_batch_date}' as date)
      GROUP BY paymentDate,
               dw_customer_id
      UNION ALL
      SELECT DATE (src_create_timestamp) AS Summary_date,
             dw_customer_id,
             0 AS order_count,
             0 AS order_opd,
             
             0 AS order_cost_amt,
             0 AS order_mrp_amount,
             0 AS product_ord_qty,
             0 AS prod_item_qty,
             0 AS cancelled_order_count,
             0 AS cancelled_order_amount,
             0 AS Cancelled_order_apd,
             0 AS shipped_ord_count,
             0 AS shipped_ord_amt,
             0 AS shipped_ord_apd,
             0 AS payment_apd,
             0 AS payment_amt,
             1 AS new_cust_apd
      FROM devdw.customers
      WHERE DATE (src_create_timestamp) >= cast('{mn.etl_batch_date}' as date)) d
GROUP BY d.Summary_date,
         d.dw_customer_id;


UPDATE devdw.daily_customer_summary dcs1
set new_customer_paid_apd = 1
from (SELECT t1.dw_customer_id,
       t1.fod
       FROM (SELECT dw_customer_id,
             MIN(summary_date) AS fod
             FROM devdw.daily_customer_summary
             WHERE order_apd = 1
             GROUP BY 1) t1
        WHERE t1.fod >= cast('{mn.etl_batch_date}' as date)) dcs2
where dcs1.dw_customer_id=dcs2.dw_customer_id and dcs1.summary_date=dcs2.fod;



"""

        # Execute the COPY command
        cursor.execute(copy_command)

        # Commit the transaction
        conn.commit()
        print(f"{table_name} successfully completed.")
        

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

# Example usage
copy_data_between_schemas('dcs')
