import psycopg2
import etl as etl

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
        copy_command = f"""delete from prod.monthly_customer_summary where start_of_the_month_date>=DATE_TRUNC('MONTH', CAST('{etl.batch_date}' AS DATE));

INSERT INTO prod.monthly_customer_summary
(
  start_of_the_month_date,
  dw_customer_id,
  order_count,
  order_apd,
  order_apm,
  order_cost_amount,
  order_mrp_amount,
  products_ordered_qty,
  products_items_qty,
  cancelled_order_count,
  cancelled_order_amount,
  cancelled_order_apd,
  cancelled_order_apm,
  shipped_order_count,
  shipped_order_amount,
  shipped_order_apd,
  shipped_order_apm,
  payment_apd,
  payment_apm,
  payment_amount,
  new_customer_apd,
  new_customer_apm,
  new_customer_paid_apd,
  new_customer_paid_apm,
  dw_create_timestamp,
  dw_update_timestamp,
  etl_batch_no,
  etl_batch_date
)
SELECT date_trunc('MONTH',d.summary_date) AS start_of_the_month,
       d.dw_customer_id,
       SUM(d.order_count) AS order_count,
       SUM(d.order_apd) AS order_apd,
       CASE
         WHEN SUM(d.order_apd) > 0 THEN 1
         ELSE 0
       END AS order_apm,
       SUM(d.order_cost_amount) AS order_cost_amount,
       SUM(d.order_mrp_amount) AS order_mrp_amount,
       SUM(d.products_ordered_qty) AS products_ordered_qty,
       SUM(d.products_items_qty) AS products_items_qty,
       SUM(d.cancelled_order_count) AS cancelled_order_count,
       SUM(d.cancelled_order_amount) AS cancelled_order_amount,
       SUM(d.cancelled_order_apd) AS cancelled_order_apd,
       CASE
         WHEN SUM(d.cancelled_order_apd) > 0 THEN 1
         ELSE 0
       END AS cancelled_order_apm,
       SUM(d.shipped_order_count) AS shipped_order_count,
       SUM(d.shipped_order_amount) AS shipped_order_amount,
       SUM(d.shipped_order_apd) AS shipped_order_apd,
       CASE
         WHEN SUM(d.shipped_order_apd) > 0 THEN 1
         ELSE 0
       END AS shipped_order_apm,
       SUM(d.payment_apd) AS payment_apd,
       CASE
         WHEN SUM(payment_apd) > 0 THEN 1
         ELSE 0
       END AS payment_apm,
       SUM(d.payment_amount) AS payment_amount,
       SUM(d.new_customer_apd) AS new_customer_apd,
       CASE
         WHEN SUM(d.new_customer_apd) > 0 THEN 1
         ELSE 0
       END AS new_customer_apm,
       SUM(d.new_customer_paid_apd) as new_customer_paid_apd,
       CASE when SUM(d.new_customer_paid_apd)>0 then 1 else 0 end as new_customer_paid_apm,
       current_timestamp,
       current_timestamp,
      {etl.batch_no},
      cast('{etl.batch_date}' as date)

FROM prod.daily_customer_summary d
where date_trunc('MONTH',d.summary_date) >= DATE_TRUNC('MONTH', CAST('{etl.batch_date}' AS DATE))

GROUP BY 1,
         2;
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
copy_data_between_schemas('prod', 'prod', 'mcs')
