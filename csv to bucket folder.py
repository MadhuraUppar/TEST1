import boto3

def files_to_bucket(bucket_name,folder_name,file_path,filename):
    s3=boto3.resource('s3')
    s3.meta.client.upload_file(file_path,bucket_name,folder_name+filename)
    print('file uploaded')

bucket_name='madhura-s3bucket'

folder_name=['customers/','employees/','offices/','orderdetails/','orders/','payments/','products/','productlines/']

file_path=[r"C:\Users\madhura.uppar\Downloads\New folder\TEST1\Customers.csv",
           r"C:\Users\madhura.uppar\Downloads\New folder\TEST1\Employees.csv",
           r"C:\Users\madhura.uppar\Downloads\New folder\TEST1\Offices.csv",
           r"C:\Users\madhura.uppar\Downloads\New folder\TEST1\Orderdetails.csv",
           r"C:\Users\madhura.uppar\Downloads\New folder\TEST1\Orders.csv",
           r"C:\Users\madhura.uppar\Downloads\New folder\TEST1\Payments.csv",
           r"C:\Users\madhura.uppar\Downloads\New folder\TEST1\Products.csv",
           r"C:\Users\madhura.uppar\Downloads\New folder\TEST1\Productlines.csv"]
for i in range(len(folder_name)):
    print('yes')
    files_to_bucket(bucket_name, folder_name[i], file_path[i],'cm14')






