import boto3

def files_to_bucket(bucket_name,folder_name,file_path,filename):
    s3=boto3.resource('s3')
    s3.meta.client.upload_file(file_path,bucket_name,folder_name+filename)
    print('file uploaded')

bucket_name='madhura-s3bucket'

folder_name=['orderdetails/']

file_path=[
           r"C:\Users\madhura.uppar\Downloads\New folder\TEST1\Orderdetails.csv"
           ]
for i in range(len(folder_name)):
    print('yes')
    files_to_bucket(bucket_name, folder_name[i], file_path[i],'cm10')






