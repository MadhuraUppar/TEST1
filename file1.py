import oracledb
import platform

un = 'g23madhura'
cs = '54.224.209.13:1521/xe'
pw = 'g23madhura123'
if platform.system() == "Windows":
  d = r"C:\Users\madhura.uppar\Downloads\instantclient-basic-windows.x64-21.12.0.0.0dbru\instantclient_21_12"
oracledb.init_oracle_client(lib_dir=d)

with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
    with connection.cursor() as cursor:
        sql = """select * from customers"""
        for r in cursor.execute(sql):
            print(r)
