import psycopg2 as pg
import boto3

# Redshift connection parameters
host = 'default-workgroup.115203216969.us-east-1.redshift-serverless.amazonaws.com'
database = 'dev'
user = 'admin'
password = 'Madhu123' # Leave this empty if using AWS CLI for authentication
port = '5439'  # Adjust the port as needed
s3 = boto3.client('s3') 
# S3 location of the data to be loaded
s3_bucket = 'madhura-s3bucket'
s3_prefix = 'offices/cm9'

# Redshift table name where data will be loaded
redshift_table = 'stage.offices'

# SQL COPY command to load data from S3 to Redshift

copy_sql = f"""
    COPY {redshift_table} 
    (officecode, city, phone, addressline1, addressline2, state, country, postalcode, territory, create_timestamp, update_timestamp) 
    FROM 's3://{s3_bucket}/{s3_prefix}' 
    IAM_ROLE 'arn:aws:iam::115203216969:role/service-role/AmazonRedshift-CommandsAccessRole-20231102T151525' 
    FORMAT AS CSV DELIMITER ',' QUOTE '"' 
    IGNOREHEADER 1 
    REGION AS 'eu-north-1';

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
