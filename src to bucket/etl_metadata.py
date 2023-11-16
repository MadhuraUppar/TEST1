import psycopg2
import sys
sys.path.append('C:/Users/madhura.uppar/Downloads/New folder/TEST1')
import mainsetvariable as mn

def etl_tables_update(schemaname,table1,table2):
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
        # Build the update command to update tables.
        update_command = f"""update {schemaname}.{table1}
    set etl_batch_no = {mn.etl_batch_n0},
        etl_batch_date = cast('{mn.etl_batch_date}' as date) ;
        
    insert into {schemaname}.{table2}(etl_batch_no,
    etl_batch_date,
    etl_batch_status,
    etl_batch_start_time) VALUES(
    {mn.etl_batch_n0},
    cast('{mn.etl_batch_date}' as date),
    'O' ,
    current_timestamp );
    """

        # Execute the COPY command
        cursor.execute(update_command)

        # Commit the transaction
        conn.commit()
        print(f"{table1} and {table2} from schema {schemaname} are updated")
        

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

# Example usage
etl_tables_update('etl_metadata','batch_control','batch_control_log')
