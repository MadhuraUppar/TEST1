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
        copy_command = f"""update prod.customers  z
set    
   customerName = s.customerName,
   contactLastName = s.contactLastName,
   contactFirstName = s.contactFirstName,
   phone  =  s.phone,
   addressLine1 = s.addressLine1,
   addressLine2 = s.addressLine2,
   city  = s.city,
   state = s.state,
   postalCode = s.postalCode,
   country = s.country,
   salesRepEmployeeNumber =  s.salesRepEmployeeNumber,
   creditLimit =  s.creditLimit,
   src_update_timestamp = s.update_timestamp,
   dw_update_timestamp = CURRENT_TIMESTAMP,
   etl_batch_no={etl.batch_no},
   etl_batch_date=cast('{etl.batch_date}' as date)
from stage.customers  s
where z.src_customerNumber = s.customerNumber;
insert into prod.customers
(
  
   src_customerNumber,
   customerName,
   contactLastName,
   contactFirstName,
   phone,
   addressLine1,
   addressLine2,
   city,
   state,
   postalCode,
   country,
   salesRepEmployeeNumber,
   creditLimit,
   src_create_timestamp,
   src_update_timestamp,
dw_create_timestamp,
dw_update_timestamp,
etl_batch_no,
etl_batch_date
)
select   s.customerNumber,
   s.customerName,
   s.contactLastName,
   s.contactFirstName,
   s.phone,
   s.addressLine1,
   s.addressLine2,
   s.city,
   s.state,
   s.postalCode,
   s.country,
   s.salesRepEmployeeNumber,
   s.creditLimit,
   s.create_timestamp,
   s.update_timestamp,
current_timestamp,
current_timestamp,
{etl.batch_no},
cast('{etl.batch_date}' as date)
from stage.customers s
left join prod.customers t
on s.customerNumber = t.src_customerNumber
where t.src_customerNumber is null;

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
copy_data_between_schemas('stage', 'prod', 'customers')
