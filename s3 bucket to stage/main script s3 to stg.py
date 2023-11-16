import subprocess

def run_another_script(script_path):
    try:
        subprocess.run(['python', script_path], check=True)
        print(f"{script_path} is runned succesfully")
    except subprocess.CalledProcessError as e:
        print(f"Error running the script: {e}")

if __name__ == "__main__":
    file_paths=['cust bucket to stage.py',
                'emp bucket to stage.py',
                'offi bucket to stage.py',
                'orderdetails bucket to stage.py',
                'orders bucket to stage.py',
                'payments bucket to stage.py',
                'productlines stage to bucket.py',
                'products bucket to stage.py'
                ]
    for i in file_paths:
        path=i
        folder = 's3 bucket to stage'
        print(f"{i}")
        script_to_run = f'{folder}/{path}' # Replace with the actual path of the script you want to run
        run_another_script(script_to_run)
