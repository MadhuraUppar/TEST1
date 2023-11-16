import subprocess

def run_another_script(script_path):
    try:
        subprocess.run(['python', script_path], check=True)
        print(f"{script_path} is uploaded to bucket")
    except subprocess.CalledProcessError as e:
        print(f"Error running the script: {e}")

if __name__ == "__main__":
    file_paths=['etl_metadata.py',
                'src to bucket offi.py',
                'src to bucket emp.py',
                'src to bucket cust.py',
                'src to bucket orders.py',
                'src to bucket orderdetails.py',
                'src to bucket payments.py',
                'src to bucket productline.py',
                'src to bucket products.py']
    for i in file_paths:
        path=i
        folder = 'src to bucket'
        print(f"{i}")
        script_to_run = f'{folder}/{path}' # Replace with the actual path of the script you want to run
        run_another_script(script_to_run)
