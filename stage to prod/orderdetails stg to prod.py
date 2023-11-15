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
        copy_command = f"""update prod.orderdetails  z
set    
   quantityOrdered = s.quantityOrdered,
   priceEach =   s.priceEach,
   orderLineNumber = s.orderLineNumber,
   src_update_timestamp = s.update_timestamp,
   dw_update_timestamp = CURRENT_TIMESTAMP,
   etl_batch_no={etl.batch_no},
   etl_batch_date= cast('{etl.batch_date}' as date) 
from stage.orderdetails  s
where z.src_orderNumber = s.orderNumber and z.src_productCode = s.productCode;  

insert into prod.orderdetails
(
  
   dw_order_id,
   dw_product_id,
   src_orderNumber,
   src_productCode,
   quantityOrdered,
   priceEach,
   orderLineNumber,
   src_create_timestamp,
   src_update_timestamp,
   dw_create_timestamp,
   dw_update_timestamp,
   etl_batch_no,
   etl_batch_date
)
select  
   o.dw_order_id, 
   p.dw_product_id, 
   s.orderNumber,
   s.productCode,
   s.quantityOrdered,
   s.priceEach,
   s.orderLineNumber,
   s.create_timestamp,
   s.update_timestamp,
   current_timestamp,
   current_timestamp,
   {etl.batch_no},
   cast('{etl.batch_date}' as date) 
from stage.orderdetails s
join prod.products p
on s.productCode = p.src_productCode
join prod.orders o
on s.orderNumber = o.src_orderNumber 
left join prod.orderdetails t
on s.orderNumber = t.src_orderNumber and s.productCode = t.src_productCode
where t.src_orderNumber is null ;
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
copy_data_between_schemas('stage', 'prod', 'orderdetails')
