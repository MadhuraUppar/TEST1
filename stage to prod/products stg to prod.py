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
        copy_command = f"""update devdw.products  z
set    
   productName =  s.productName,
   productLine = s.productLine,
   productScale = s.productScale,
   productVendor = s.productVendor,
   productDescription = s.productDescription,
   quantityInStock = s.quantityInStock,
   buyPrice =  s.buyPrice,
   MSRP = s.MSRP,
   dw_product_line_id = w.dw_product_line_id,
   src_update_timestamp = s.update_timestamp,
   dw_update_timestamp = CURRENT_TIMESTAMP,
   etl_batch_no={mn.etl_batch_n0},
   etl_batch_date= cast('{mn.etl_batch_date}' as date)
from devstage.products  s
join devdw.productlines w
on s.productLine = w.productLine
where z.src_productCode = s.productCode;

insert into devdw.products
(
   src_productCode,
   productName,
   productLine,
   productScale,
   productVendor,
   productDescription,
   quantityInStock,
   buyPrice,
   MSRP,
   dw_product_line_id,
   src_create_timestamp,
   src_update_timestamp,
   dw_create_timestamp,
   dw_update_timestamp,
   etl_batch_no,
   etl_batch_date
   
)
select  
   s.productCode,
   s.productName,
   s.productLine,
   s.productScale,
   s.productVendor,
   s.productDescription,
   s.quantityInStock,
   s.buyPrice,
   s.MSRP,
   w.dw_product_line_id,
   s.create_timestamp,
   s.update_timestamp,
   current_timestamp,
   current_timestamp,
   {mn.etl_batch_n0},
   cast('{mn.etl_batch_date}' as date)
from devstage.products s
join devdw.productlines w
on s.productLine = w.productLine
left join devdw.products t
on s.productCode = t.src_productCode
where t.src_productCode is null ;
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
copy_data_between_schemas('stage', 'prod', 'products')
