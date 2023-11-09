import oracledb
import csv

table_names=["Offices", "Productlines", "Employees", "Customers", "Payments","Orders","Products", "Orderdetails"]
d = r"C:\Users\madhura.uppar\Downloads\instantclient-basic-windows.x64-21.12.0.0.0dbru\instantclient_21_12"
oracledb.init_oracle_client(lib_dir=d)
connection = oracledb.connect('g23madhura/g23madhura123@54.224.209.13:1521/xe')
cursor = connection.cursor()

for table_name in table_names:
    cursor.execute(f"SELECT * FROM CM_20050614.{table_name} where to_char(update_timestamp,'yyyy-mm-dd')>='2005-06-14' ")

    column_names = [i[0] for i in cursor.description]
    rows = cursor.fetchall()
    with open(f'{table_name}.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)

        csv_writer.writerow(column_names)

        csv_writer.writerows(rows)