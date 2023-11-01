import boto3

def create_s3_bucket(bucket_name, region='us-east-1'):
   
    s3 = boto3.client('s3', region_name=region)

    
    try:
        s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})
        print(f"S3 bucket '{bucket_name}' created successfully.")
    except Exception as e:
        print(f"Error creating S3 bucket: {e}")


bucket_name = 'madhura-s3bucket'


aws_region = 'eu-north-1'


create_s3_bucket(bucket_name, aws_region)
