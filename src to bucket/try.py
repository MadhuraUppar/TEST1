import psycopg2 
import oracledb

d = r"C:\Users\madhura.uppar\Downloads\instantclient-basic-windows.x64-21.12.0.0.0dbru\instantclient_21_12"
oracledb.init_oracle_client(lib_dir=d)
connection = oracledb.connect('g23madhura/g23madhura123@54.224.209.13:1521/xe')

cursor = connection.cursor()
copy = f"""select productline from productlines@madhura_dblink"""

cursor.execute(copy)
columnnames=cursor.description
row=cursor.fetchall()
print(row)
print(columnnames)
