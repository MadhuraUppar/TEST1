import concurrent.futures
import subprocess

def run_script(script_path):
    try:
        subprocess.run(['python', script_path], check=True)
        
    except subprocess.CalledProcessError as e:
        print(f"Error running the script {script_path}: {e}")

if __name__== "__main__":
    file_paths=['s3 bucket to stage/cust bucket to stage.py',
                's3 bucket to stage/emp bucket to stage.py',
                's3 bucket to stage/offi bucket to stage.py',
                's3 bucket to stage/orderdetails bucket to stage.py',
                's3 bucket to stage/orders bucket to stage.py',
                's3 bucket to stage/payments bucket to stage.py',
                's3 bucket to stage/productlines stage to bucket.py',
                's3 bucket to stage/products bucket to stage.py'
                ]
     
    with concurrent.futures.ThreadPoolExecutor() as executor:
        
        executor.map(run_script, file_paths)
