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
        copy_command = f"""delete from prod.monthly_product_summary where start_of_the_month_date>=DATE_TRUNC('MONTH', CAST('{etl.batch_date}' AS DATE));

INSERT INTO prod.monthly_product_summary
(
  start_of_the_month_date,
  dw_product_id,
  customer_apd,
  customer_apm,
  product_cost_amount,
  product_mrp_amount,
  cancelled_product_qty,
  cancelled_cost_amount,
  cancelled_mrp_amount,
  cancelled_order_apd,
  cancelled_order_apm,
  dw_create_timestamp,
  dw_update_timestamp,
  etl_batch_no,
  etl_batch_date
)
SELECT date_trunc('MONTH',summary_date) AS start_of_the_month_date,
       dw_product_id,
       SUM(customer_apd) AS customer_apd,
       CASE
         WHEN SUM(customer_apd) > 0 THEN 1
         ELSE 0
       END customer_apm,
       SUM(product_cost_amount) AS product_cost_amount,
       SUM(product_mrp_amount) AS product_mrp_amount,
       SUM(cancelled_product_qty) AS cancelled_product_qty,
       SUM(cancelled_cost_amount) AS cancelled_cost_amount,
       SUM(cancelled_mrp_amount) AS cancelled_mrp_amount,
       SUM(cancelled_order_apd) AS cancelled_order_apd,
       CASE
         WHEN SUM(cancelled_order_apd) > 0 THEN 1
         ELSE 0
       END AS cancelled_order_apm,
       current_timestamp,
       current_timestamp,
       {etl.batch_no},
       cast('{etl.batch_date}' as date)
FROM prod.daily_product_summary
where date_trunc('MONTH',summary_date)>=DATE_TRUNC('MONTH', CAST('{etl.batch_date}' AS DATE))
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
copy_data_between_schemas('prod', 'prod', 'mps')
