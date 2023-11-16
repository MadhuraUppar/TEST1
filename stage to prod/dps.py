import psycopg2
import sys
sys.path.append('C:/Users/madhura.uppar/Downloads/New folder/TEST1')
import mainsetvariable as mn

def copy_data_between_schemas(source_schema, target_schema, table_name):
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
        copy_command = f"""insert into prod.daily_product_summary
(summary_date,
dw_product_id,
customer_apd,
product_cost_amount,
product_mrp_amount,
cancelled_product_qty,
cancelled_cost_amount,
cancelled_mrp_amount,
cancelled_order_apd,
dw_create_timestamp,
dw_update_timestamp,
etl_batch_no,
etl_batch_date)
SELECT d.summary_date,
       d.dw_product_id,
       MAX(d.customer_apd) AS customer_apd,
       SUM(d.product_cost_amount) AS product_cost_amount,
       SUM(d.product_mrp_amount) AS product_mrp_amount,
       SUM(d.cancelled_product_qty) AS cancelled_product_qty,
       SUM(d.cancelled_cost_amount) AS cancelled_cost_amount,
       SUM(d.cancelled_mrp_amount) AS cancelled_mrp_amount,
       MAX(d.cancelled_order_apd) AS cancelled_order_apd,
       current_timestamp,
       current_timestamp,
       {mn.etl_batch_n0},
       cast('{mn.etl_batch_date}' as date)
FROM (SELECT o.orderdate AS summary_date,
             od.dw_product_id,
             1 AS customer_apd,
             SUM(od.quantityOrdered*od.priceEach) AS product_cost_amount,
             SUM(od.quantityOrdered*p.MSRP) AS product_mrp_amount,
             0 AS cancelled_product_qty,
             0 AS cancelled_cost_amount,
             0 AS cancelled_mrp_amount,
             0 AS cancelled_order_apd
      FROM prod.orderdetails od
        JOIN prod.orders o ON od.dw_order_id = o.dw_order_id
        JOIN prod.products p ON p.dw_product_id = od.dw_product_id
      WHERE o.orderdate >= cast('{mn.etl_batch_date}' as date)
      GROUP BY o.orderdate,
               od.dw_product_id
      UNION ALL
      SELECT o.cancelledDate AS summary_date,
             od.dw_product_id,
             1 AS customer_apd,
             0 AS product_cost_amount,
             0 AS product_mrp_amount,
             COUNT(p.dw_product_id) AS cancelled_product_qty,
             SUM(od.quantityOrdered*od.priceEach) AS cancelled_cost_amount,
             SUM(od.quantityOrdered*p.MSRP) AS cancelled_mrp_amount,
             1 AS cancelled_order_apd
      FROM prod.orderdetails od
        JOIN prod.orders o ON od.dw_order_id = o.dw_order_id
        JOIN prod.products p ON p.dw_product_id = od.dw_product_id
      WHERE o.cancelledDate >= cast('{mn.etl_batch_date}' as date)
      GROUP BY o.cancelledDate,
               od.dw_product_id) d
GROUP BY d.summary_date,
         d.dw_product_id;
"""

        # Execute the COPY command
        cursor.execute(copy_command)

        # Commit the transaction
        conn.commit()
        print(f"Data from {source_schema}.{table_name} copied to {target_schema}.{table_name} successfully.")
        

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

# Example usage
copy_data_between_schemas('prod', 'prod', 'dps')
