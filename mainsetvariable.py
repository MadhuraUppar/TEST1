import psycopg2

host = 'default-workgroup.115203216969.us-east-1.redshift-serverless.amazonaws.com'
port = '5439'
user = 'admin'
password = 'Madhu123'
dbname = 'dev'

# Establish connection
conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
cursor = conn.cursor()

command=f""" select etl_batch_no,etl_batch_date from etl_metadata.batch_control"""
cursor.execute(command)
row=cursor.fetchall()
etl_batch_date=row[0][1]
etl_batch_no=row[0][0]