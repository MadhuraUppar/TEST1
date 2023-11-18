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
        copy_command = f"""update devdw.customer_history a
set 
effective_to_date = DATEADD(day,-1,cast('{mn.etl_batch_date}' as date)),
dw_active_record_ind = 0,
dw_update_timestamp = current_timestamp,
update_etl_batch_no= {mn.etl_batch_no},
update_etl_batch_date= cast('{mn.etl_batch_date}' as date)
from devdw.customers b
where a.dw_customer_id = b.dw_customer_id 
      and a.dw_active_record_ind=1 
      and a.creditlimit<>b.creditlimit;

insert into devdw.customer_history 
( dw_customer_id,
creditlimit,
effective_from_date,
dw_active_record_ind,
dw_create_timestamp,
dw_update_timestamp,
create_etl_batch_no,
create_etl_batch_date)
select d.dw_customer_id,
d.creditlimit,
cast('{mn.etl_batch_date}' as date),
1 dw_active_record_ind,
current_timestamp,
current_timestamp,
{mn.etl_batch_no},
cast('{mn.etl_batch_date}' as date)
from devdw.customers d left join (select dw_customer_id from devdw.customer_history  where dw_active_record_ind = 1) g 
on d.dw_customer_id = g.dw_customer_id
where g.dw_customer_id is null;
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
copy_data_between_schemas('customer_history')
