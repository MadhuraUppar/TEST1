import subprocess
import concurrent.futures

def run_another_script(script_path):
    try:
        subprocess.run(['python', script_path], check=True)
        
    except subprocess.CalledProcessError as e:
        print(f"Error running the script: {e}")

if __name__ == "__main__":
    file_paths=[['stage to prod/offi stage to prod.py','stage to prod/productlines stg to prod.py'],
                ['stage to prod/emp stg to prod.py','stage to prod/products stg to prod.py'],
                ['stage to prod/cust stg to prod.py','stage to prod/prodhis stg to prod.py'],
                ['stage to prod/payments stg to prod.py',
                'stage to prod/cushis stg to prod.py',
                'stage to prod/orders stg to prod.py'],
                ['stage to prod/orderdetails stg to prod.py'],
                ['stage to prod/dcs.py','stage to prod/dps.py'],
                ['stage to prod/mcs.py','stage to prod/mps.py'],
                ['stage to prod/etlmetadata update.py']]
    
    #ThreadPoolExecutor for parallel execution
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for files in file_paths:
            #submit the scripts for execution
            scripts ={executor.submit(run_another_script, script): script for script in files}
            for i in concurrent.futures.as_completed(scripts):
                #Wait for the scripts to complete and get the result
                result = i.result()
        
                

