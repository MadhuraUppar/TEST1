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
        copy_command = f"""update prod.product_history a 
set effective_to_date=DATEADD(day, 1,cast('{etl.batch_date}' as date)),
dw_active_record_ind=0,
dw_update_timestamp=current_timestamp,
update_etl_batch_date=cast('{etl.batch_date}' as date),
update_etl_batch_no={etl.batch_no}
from prod.products b
where a.dw_product_id=b.dw_product_id and a.dw_active_record_ind=1 and a.MSRP<>b.MSRP;

insert into prod.product_history(
dw_product_id,
MSRP,
effective_from_date,
dw_active_record_ind,
dw_create_timestamp,
dw_update_timestamp,
create_etl_batch_no,
create_etl_batch_date)
select d.dw_product_id,
d.MSRP,
cast('{etl.batch_date}' as date),
1,
current_timestamp,
current_timestamp,
{etl.batch_no},
cast('{etl.batch_date}' as date)
from prod.products d left join(select dw_product_id from prod.product_history where dw_active_record_ind=1) g
on d.dw_product_id=g.dw_product_id
where g.dw_product_id is null;
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
copy_data_between_schemas('stage', 'prod', 'product_history')
