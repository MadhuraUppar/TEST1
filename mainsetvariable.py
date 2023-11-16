import oracledb

# Set up Oracle and S3 connection
d = r"C:\Users\madhura.uppar\Downloads\instantclient-basic-windows.x64-21.12.0.0.0dbru\instantclient_21_12"
oracledb.init_oracle_client(lib_dir=d)

connection = oracledb.connect('g23madhura/g23madhura123@54.224.209.13:1521/xe')
cursor = connection.cursor()

schema_name = 'cm_20050614'
identified = 'cm_20050614123'
etl_batch_date = '2005-06-14'
etl_batch_n0 = 1001
cursor.execute(f'Drop public database link madhura_dblink')

query= f"CREATE PUBLIC database link madhura_dblink CONNECT TO {schema_name} IDENTIFIED BY {identified} USING 'XE'"

cursor.execute(query)

connection.close
