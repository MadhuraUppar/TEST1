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
        copy_command = f"""update prod.offices o1
set 
officecode=o2.officecode,
city=o2.city,
phone=o2.phone,
addressline1=o2.addressline1,
addressline2=o2.addressline2,
state=o2.state,
country=o2.country,
postalcode=o2.postalcode,
territory=o2.territory,
src_create_timestamp=o2.create_timestamp,
src_update_timestamp=o2.update_timestamp,
dw_update_timestamp= current_timestamp,
etl_batch_no = {etl.batch_no},
etl_batch_date= cast('{etl.batch_date}' as date)
from stage.offices o2
where o1.officecode=o2.officecode;

insert into prod.offices
(officecode,
city,
phone,
addressline1,
addressline2,
state,
country,
postalcode,
territory,
src_create_timestamp,
src_update_timestamp,
dw_create_timestamp,
dw_update_timestamp,
etl_batch_no,
etl_batch_date
)
select 
o1.officecode,
o1.city,
o1.phone,
o1.addressline1,
o1.addressline2,
o1.state,
o1.country,
o1.postalcode,
o1.territory,
o1.create_timestamp,
o1.update_timestamp,
current_timestamp,
current_timestamp,
{etl.batch_no},
cast('{etl.batch_date}' as date)
from
stage.offices o1
left join prod.offices o2 
on o1.officecode=o2.officecode
where o2.officecode is null;
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
copy_data_between_schemas('stage', 'prod', 'offices')
