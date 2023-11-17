import concurrent.futures
import subprocess

def run_script(script_path):
    try:
        subprocess.run(['python', script_path], check=True)
        print(f"{script_path} uploaded successfully to bucket")
    except subprocess.CalledProcessError as e:
        print(f"Error running the script {script_path}: {e}")

if __name__== "__main__":
    file_paths=["src to bucket/etl_metadata.py",
                "src to bucket/src to bucket offi.py",
                "src to bucket/src to bucket emp.py",
                "src to bucket/src to bucket cust.py",
                "src to bucket/src to bucket orders.py",
                "src to bucket/src to bucket orderdetails.py",
                "src to bucket/src to bucket payments.py",
                "src to bucket/src to bucket productline.py",
                "src to bucket/src to bucket products.py"]
     
    with concurrent.futures.ThreadPoolExecutor() as executor:
        
        executor.map(run_script, file_paths)
