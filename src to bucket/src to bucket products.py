import oracledb
import boto3
import io
import csv
import sys
sys.path.append('C:/Users/madhura.uppar/Downloads/New folder/TEST1')
import mainsetvariable as mn

print('Execution started')
# Set up Oracle and S3 connection
d = r"C:\Users\madhura.uppar\Downloads\instantclient-basic-windows.x64-21.12.0.0.0dbru\instantclient_21_12"
oracledb.init_oracle_client(lib_dir=d)
connection = oracledb.connect('g23madhura/g23madhura123@54.224.209.13:1521/xe')

s3 = boto3.client('s3')
bucket_name = 'madhura-s3bucket'
table_name = 'products'
etl_batch_date = mn.etl_batch_date


try: 
    cursor = connection.cursor()

    query = f'''SELECT PRODUCTCODE,
                       PRODUCTNAME,
                       PRODUCTLINE,
                       PRODUCTSCALE,
                       PRODUCTVENDOR,
                       QUANTITYINSTOCK,
                       BUYPRICE,
                       MSRP,
                       CREATE_TIMESTAMP,
                       UPDATE_TIMESTAMP

                FROM {table_name}@madhura_dblink 
                WHERE To_Char(update_timestamp, 'YYYY-MM-DD') >= '{etl_batch_date}'
                '''
    cursor.execute(query)

    # Fetch column names and data
    col_names = [i[0] for i in cursor.description]
    print(col_names)
    rows = cursor.fetchall()

    csv_data = io.StringIO()
    csv_writer = csv.writer(csv_data)
    csv_writer.writerow(col_names)
    csv_writer.writerows(rows)

    # Encode the CSV data as bytes using UTF-8
    csv_bytes = csv_data.getvalue().encode('utf-8')

    # Upload the CSV data to S3
    s3_path = f'{table_name}/{etl_batch_date}/{table_name}.csv'
    s3.put_object(Bucket=bucket_name, Key=s3_path, Body=csv_bytes)

    print(f'Uploaded {etl_batch_date}.{table_name} to S3')
except Exception as e:
    print(f'Failed to upload to S3: {str(e)}')

# Close the Oracle connection
connection.close()