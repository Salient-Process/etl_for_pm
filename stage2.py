import re
import os
import zipfile
import logging 
import requests


def process_csv_file(directory,org_key,url,projectKey,user,process_mining_api_key,file_path):
    
    api_key = process_mining_api_key
    org_key = org_key
    username = user
    # Getting the Process Mining API key from configuration
    process_mining_api_key = api_key #config["process-mining"]["api-key"]
    user = username #config["process-mining"]["user"]
    url = url
    org_key = org_key #config["process-mining"]["org-key"]

    base_csv_file_name = os.path.basename(file_path)
    
    #Remove verify = False
    #getFileAndZip(base_csv_file_name,file_path)

    upload_csv_and_createlog(base_csv_file_name,projectKey,org_key,file_path,directory,url,user,process_mining_api_key)
    # Throw artificial error to debug error processing
    if re.search("error", base_csv_file_name, re.IGNORECASE):
        raise Exception("Simulated exception because the CSV file name contains the word 'error'") 
    
    return

def getFileAndZip(file,path,work_dir):
    zipPath = os.path.join(work_dir,file+'.zip')
    zip_file = zipfile.ZipFile(zipPath, 'w')
    zip_file.write(path)
    zip_file.close()
    fileName = file+'.zip'
    logging.info("FileName: "+fileName)
    return fileName

def createKey(url,user,apikey):
    logging.info('Creating Token')
    api_url = '%s/integration/sign'%url
    body = {'uid':user,'apiKey':apikey}
    response = requests.post(api_url, json=body)
    if response.json()['success']:
        return response.json()['sign']
    else:
        return 0

def uploadFile(file,url,projectkey,orgkey,path,work_dir,user,apikey):
    
    fileName = getFileAndZip(file,path,work_dir)
    logging.info("FileName: "+fileName)
    token = createKey(url,user,apikey)
    if token == 0:
        raise Exception('Was not posible to create the token')
    else:
        zipPath = os.path.join(work_dir,fileName)
        api_url = "%s/integration/csv/%s/upload" % (url, projectkey)
        headers = {'Authorization': 'Bearer %s'% token}
        files = {'file': (fileName, open(zipPath, 'rb'),'text/zip')}
        param = {'org': orgkey}
        upload = requests.post(api_url,files=files,params=param, headers=headers)
        logging.info("upload: " +str(upload.json()))
        jobId = upload.json()['data']
        logging.info("JobId: " +jobId)
        if upload.json()['success']:
            return jobId
        else:
            return 0

def createLog(url,projectkey,orgkey,user,apikey):
    token = createKey(url,user,apikey)
    if token == 0:
        raise Exception('Was not posible to create the token')
    else:
        api_url = "%s/integration/csv/%s/create-log" % (url, projectkey)
        param = {'org': orgkey}
        headers = {'Authorization': 'Bearer %s'% token}
        response = requests.post(api_url,headers=headers, params=param)
        jobId = response.json()['data']
        if response.json()['success']:
            return jobId
        else:
            return 0
def jobStatus(jobKey,url,user,apikey):
    token = createKey(url,user,apikey)
    if token == 0:
        raise Exception('Was not posible to create the token')
    else:
        api_url = "%s/integration/csv/job-status/%s" % (url, jobKey)
        headers = {'Authorization': 'Bearer %s'%token}
        response = requests.get(api_url,headers=headers)
        if response.json()['success']:
            return response.json()
        else:
            return 0
def upload_csv_and_createlog(file,projectkey,orgkey,path,work_dir,url,user,apikey):
    
    r = uploadFile(file,url,projectkey,orgkey,path,work_dir,user,apikey)
    logging.info('Response: '+r)
    if r == 0:
        raise Exception('Was not posible to upload the file')
    else:
        # wait until async call is completed
        jobKey = r
        runningCall = 1
        while runningCall:
            r = jobStatus(jobKey,url,user,apikey)
            if (r['data'] == 'complete'):
                runningCall = 0
            if (r['data'] == 'error'):
                runningCall = 0
                raise Exception("Error while loading CSV -- column number mismatch "+r)
    
    r = createLog(url,projectkey,orgkey,user,apikey)
    if r == 0:
        raise Exception('Was not posible to create the log')
    else:
        # wait until async call is completed
        jobKey = r
        runningCall = 1
        while runningCall:
            r = jobStatus(jobKey,url,user,apikey)
            if (r['data'] == 'complete'):
                runningCall = 0
            if (r['data'] == 'error'):
                runningCall = 0
    return r
