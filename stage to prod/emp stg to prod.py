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
        copy_command = f"""update prod.employees z
set    
   lastName =  s.lastName,
   firstName =  s.firstName,
   extension =   s.extension,
   email = s.email,
   officeCode = s.officeCode,
   reportsTo =  s.reportsTo,
   jobTitle =  s.jobTitle,
   dw_office_id =  w.dw_office_id,
   src_update_timestamp = s.update_timestamp,
   dw_update_timestamp = CURRENT_TIMESTAMP,
   etl_batch_no={mn.etl_batch_n0},
   etl_batch_date=cast('{mn.etl_batch_date}' as date)
from stage.employees s
join prod.offices w
on s.officecode = w.officecode
where z.employeenumber = s.employeenumber;
update prod.employees e1
set dw_reporting_employee_id = e2.dw_employee_id,  
    dw_update_timestamp = CURRENT_TIMESTAMP
from prod.employees e2
where e1.reportsto = e2.employeenumber;



insert into prod.employees
(
  
   employeenumber,
   lastname,
   firstname,
   extension,
   email,
   officecode,
   reportsto,
   jobtitle,
   dw_office_id,
   src_create_timestamp,
   src_update_timestamp,
   dw_create_timestamp,
   dw_update_timestamp,
   etl_batch_no,
   etl_batch_date

)
select    s.employeenumber,
          s.lastname,
          s.firstname,
          s.extension,
          s.email,
          s.officecode,
          s.reportsto,
          s.jobtitle,
          w.dw_office_id,
          s.create_timestamp,
          s.update_timestamp,
          current_timestamp,
          current_timestamp,
          {mn.etl_batch_n0},
          cast('{mn.etl_batch_date}' as date)
from stage.employees s
join prod.offices w
on s.officecode = w.officecode
left join prod.employees t
on s.employeenumber = t.employeenumber
where t.employeenumber is null ;

update prod.employees e1
set dw_reporting_employee_id = e2.dw_employee_id,  
    dw_update_timestamp = CURRENT_TIMESTAMP
from prod.employees e2
where e1.reportsTo = e2.employeeNumber and e1.dw_reporting_employee_id is null and e1.reportsTo is not null;
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
copy_data_between_schemas('stage', 'prod', 'employees')
