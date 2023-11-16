import psycopg2 as pg
import boto3
import sys
sys.path.append('C:/Users/madhura.uppar/Downloads/New folder/TEST1')
import mainsetvariable as mn

# Redshift connection parameters
host = 'default-workgroup.115203216969.us-east-1.redshift-serverless.amazonaws.com'
database = 'dev'
user = 'admin'
password = 'Madhu123' # Leave this empty if using AWS CLI for authentication
port = '5439'  # Adjust the port as needed
s3 = boto3.client('s3') 
# S3 location of the data to be loaded
s3_bucket = 'madhura-s3bucket'
table_name='customers'



# Redshift table name where data will be loaded
redshift_table = 'stage.customers'

# SQL COPY command to load data from S3 to Redshift

copy_sql = f"""
    COPY {redshift_table} (customernumber, customername, contactlastname, contactfirstname, phone, addressline1, addressline2, city, state, postalcode, country, salesrepemployeenumber, creditlimit, create_timestamp, update_timestamp) 
    FROM 's3://{s3_bucket}/{table_name}/{mn.etl_batch_date}/{table_name}'
    IAM_ROLE 'arn:aws:iam::115203216969:role/service-role/AmazonRedshift-CommandsAccessRole-20231102T151525' 
    FORMAT AS CSV DELIMITER ',' QUOTE '"' ACCEPTINVCHARS '_' 
    IGNOREHEADER 1 EMPTYASNULL 
    REGION AS 'eu-north-1'
"""
try:
    # Connect to Redshift
    conn = pg.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
    )
    cursor = conn.cursor()

    # Execute the COPY command to load data from S3
    cursor.execute(copy_sql)
    conn.commit()

    print("Data loaded successfully into Redshift.")

except Exception as e:
    print(f"Error: {str(e)}")
#conn.close()
