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
        copy_command = f"""update devdw.payments z
set    
   paymentDate =  s.paymentDate,
   amount =  s.amount,
   src_update_timestamp = s.update_timestamp,
   dw_update_timestamp = CURRENT_TIMESTAMP,
   etl_batch_no={mn.etl_batch_no},
   etl_batch_date=cast('{mn.etl_batch_date}' as date)
from devstage.payments  s
where z.src_customerNumber = s.customerNumber  and  z.checkNumber =  s.checkNumber;

insert into devdw.payments
(
  dw_customer_id,
   src_customerNumber,
   checkNumber,
   paymentDate,
   amount,
   src_create_timestamp,
   src_update_timestamp,
   dw_create_timestamp,
   dw_update_timestamp,
   etl_batch_no,
   etl_batch_date
)
select   w.dw_customer_id,
   s.customerNumber,
   s.checkNumber,
   s.paymentDate,
   s.amount,
   s.create_timestamp,
   s.update_timestamp,
   current_timestamp,
   current_timestamp,
   {mn.etl_batch_no},
   cast('{mn.etl_batch_date}' as date)

from devstage.payments s
join devdw.customers w
on s.customerNumber = w.src_customerNumber
left join devdw.payments t
on s.customerNumber = t.src_customerNumber  and s.checkNumber = t.checkNumber
where t.checkNumber is null ;
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
copy_data_between_schemas('stage', 'prod', 'payments')
