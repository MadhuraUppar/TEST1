import subprocess

def run_another_script(script_path):
    try:
        subprocess.run(['python', script_path], check=True)
        print(f"{script_path} is uploaded to prod")
    except subprocess.CalledProcessError as e:
        print(f"Error running the script: {e}")

if __name__ == "__main__":
    file_paths=['offi stage to prod.py',
                'emp stg to prod.py',
                'cust stg to prod.py',
                'productlines stg to prod.py',
                'products stg to prod.py',
                'prodhis stg to prod.py',
                'payments stg to prod.py',
                'cushis stg to prod.py',
                'orders stg to prod.py',
                'orderdetails stg to prod.py',
                'dcs.py',
                'mcs.py',
                'dps.py',
                'mps.py',
                'etlmetadata update.py']
    for i in file_paths:
        path=i
        folder = 'stage to prod'
        print(f"{i}")
        script_to_run = f'{folder}/{path}' # Replace with the actual path of the script you want to run
        run_another_script(script_to_run)
