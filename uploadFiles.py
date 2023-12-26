import os
from stage2 import process_csv_file
import logging


def sendtoPrM(directory,org_key,user,process_mining_api_key,config):
    url = config['process-mining']['url']
    files =  os.listdir(directory)
    for file in files:
        csv_file_path = os.path.join(directory, file)
        
        if 'CurrentOrder' in file:
            projectKey = config['process-mining']['project-key-order']
        elif 'IntransitItem' in file:
            projectKey = config['process-mining']['project-key-instransit']
        elif 'Sonoco' in file:
            projectKey = config['process-mining']['project-key-sonoco']
        else:
            projectKey = config['process-mining']['project-key-digital']

        logging.info(f"FileName : {file}")
        logging.info(f"ProjectKey: {projectKey}")
        process_csv_file(directory,org_key,url,projectKey,user,process_mining_api_key,csv_file_path)
        os.remove(os.path.join(directory,file))
        os.remove(os.path.join(directory,file+'.zip'))


if( __name__ == "__main__"):
    sendtoPrM()