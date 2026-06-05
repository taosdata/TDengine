import os
import subprocess

class BuildTDengine:
    def __init__(self, host='vm96', path = '/root/pxiao/TDengine', branch = 'main') -> None:
        self.host = host
        self.path = path
        self.branch = branch
    
    def build(self):
        parameters=[self.path, self.branch]
        build_fild = "./build.sh"
        try:
            # Run the Bash script using subprocess
            subprocess.run(['bash', build_fild] + parameters, check=True)
            print("TDengine build successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error running Bash script: {e}")
        except FileNotFoundError as e:
            print(f"File not found: {e}")
    
    def get_cmd_output(self, cmd):        
        try:
            # Run the Bash command and capture the output
            result = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, text=True)
            
            # Access the output from the 'result' object
            output = result.stdout
            
            return output.strip()
        except subprocess.CalledProcessError as e:
            print(f"Error running Bash command: {e}")