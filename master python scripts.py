import subprocess

file_paths=[
         "src to bucket/main script src to buc.py",
         "s3 bucket to stage/main script s3 to stg.py",
         "stage to prod/main script stg to prod.py"
         ]

def run_scripts(scripts):
    for script in scripts:
        try:
            print(f"{script} started running")
            subprocess.run(['python',script],check=True)
            print(f"{script} runned successfully")
        except subprocess.CalledProcessError as e:
            print(f"There was error in running the script {script}:{e}")
            break
run_scripts(file_paths)
        

