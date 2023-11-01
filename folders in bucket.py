import boto3

def create_s3_folder(bucket_name, folder_name):
    s3 = boto3.client('s3')
    if not folder_name.endswith('/'):
        folder_name += '/'
    try:
        s3.put_object(Bucket=bucket_name, Key=folder_name)
        print(f"Folder '{folder_name}' created successfully in S3 bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Error creating folder: {e}")


bucket_name = 'madhura-s3bucket'
folders=['customers','offices','employees','orders','orderdetails','payments','products','productlines']

for i in folders:
    folder_name = i
    create_s3_folder(bucket_name, folder_name)
