import subprocess

def run_another_script(script_path):
    try:
        subprocess.run(['python', script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running the script: {e}")

if __name__ == "__main__":
    script_to_run = 'C:/Users/madhura.uppar/Downloads/New folder/TEST1/src to bucket/src to bucket emp.py'  # Replace with the actual path of the script you want to run
    run_another_script(script_to_run)
