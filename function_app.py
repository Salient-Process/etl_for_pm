import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os
import yaml
import logging
import datetime
from script_util import dowlload_blob,setblobName
from uploadFiles import sendtoPrM

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="stage2/sendToPrM.trigger.txt",
                               connection="AzureWebJobsStorage") 
def sendCurrentOrderToPrM(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, "stage2.yaml"), "r") as f:
        stage2_config = yaml.load(f, Loader=yaml.FullLoader)

    connection_string = os.environ['AzureWebJobsStorage']
    org_key = os.environ['orgKey']
    user = os.environ['user']
    process_mining_api_key = os.environ['apiKey']

    container_name = stage2_config['bucket-directories']['container']
    stage2_input_dir = stage2_config["directories"]["input"]
    currentOrder = stage2_config['directories']['currentOrder']

    currentOderDirectory =  os.path.join(stage2_input_dir,currentOrder)

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    input_container = blob_service_client.get_container_client(container=container_name)
    blob_list = input_container.list_blobs(name_starts_with=currentOrder)
    if not os.path.exists(currentOderDirectory): os.makedirs(currentOderDirectory)
    
    for blob in blob_list:

        logging.info("[{}]:[INFO] : Blob name: {}".format(datetime.datetime.utcnow(), blob.name))
        #check if the path contains a folder structure, create the folder structure
        blob_name = setblobName(blob,currentOrder)
        blob_client = input_container.get_blob_client(blob.name)
        dowlload_blob(blob_client,currentOderDirectory+ "/"+blob_name)

    blobCurrent = input_container.get_blob_client("sendToPrM.trigger.txt")
    sendtoPrM(currentOderDirectory,org_key,user,process_mining_api_key,stage2_config)
    
    if blobCurrent.exists():
        logging.debug(f"Deleting bucket file: sendToPrM.trigger.txt")
        blobCurrent.delete_blob()
    
    blob_client = blob_service_client.get_blob_client(container="stage2/Intransit", blob="sendToPrM.trigger.txt")
    input_stream = 'Trigger Instrnsit'
    blob_client.upload_blob(input_stream, blob_type="BlockBlob")
    



@app.blob_trigger(arg_name="myblob", path="stage2/Intransit/sendToPrM.trigger.txt",
                               connection="AzureWebJobsStorage")

def sendIntransitToPrM(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, "stage2.yaml"), "r") as f:
        stage2_config = yaml.load(f, Loader=yaml.FullLoader)

    connection_string = os.environ['AzureWebJobsStorage']
    org_key = os.environ['orgKey']
    user = os.environ['user']
    process_mining_api_key = os.environ['apiKey']

    container_name = stage2_config['bucket-directories']['container']
    stage2_input_dir = stage2_config["directories"]["input"]
    instransit = stage2_config['directories']['instransit']
    intransitContainer = stage2_config['bucket-directories']['instransit']

    intransitDirectory =  os.path.join(stage2_input_dir,instransit)

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    input_container = blob_service_client.get_container_client(container=intransitContainer)
    blobCurrent = input_container.get_blob_client("sendToPrM.trigger.txt")
    
    if blobCurrent.exists():
        logging.debug(f"Deleting bucket file: sendToPrM.trigger.txt")
        blobCurrent.delete_blob()
    
    input_container = blob_service_client.get_container_client(container=container_name)
    blob_list = input_container.list_blobs(name_starts_with=instransit)

    if not os.path.exists(intransitDirectory): os.makedirs(intransitDirectory)
    
    for blob in blob_list:

        logging.info("[{}]:[INFO] : Blob name: {}".format(datetime.datetime.utcnow(), blob.name))
        #check if the path contains a folder structure, create the folder structure
        blob_name = setblobName(blob,instransit)
        blob_client = input_container.get_blob_client(blob.name)
        dowlload_blob(blob_client,intransitDirectory+ "/"+blob_name)
    
    sendtoPrM(intransitDirectory,org_key,user,process_mining_api_key,stage2_config)
    
    blob_client = blob_service_client.get_blob_client(container="stage2/Digital", blob="sendToPrM.trigger.txt")
    input_stream = 'Trigger Instrnsit'
    blob_client.upload_blob(input_stream, blob_type="BlockBlob")


@app.blob_trigger(arg_name="myblob", path="stage2/Digital/sendToPrM.trigger.txt",
                               connection="AzureWebJobsStorage") 
def sendDigitaltoPrM(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, "stage2.yaml"), "r") as f:
        stage2_config = yaml.load(f, Loader=yaml.FullLoader)

    connection_string = os.environ['AzureWebJobsStorage']
    org_key = os.environ['orgKey']
    user = os.environ['user']
    process_mining_api_key = os.environ['apiKey']

    container_name = stage2_config['bucket-directories']['container']
    stage2_input_dir = stage2_config["directories"]["input"]
    digital = stage2_config['directories']['digital']
    digitalContainer = stage2_config['bucket-directories']['digital']

    digitalDirectory =  os.path.join(stage2_input_dir,digital)

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    input_container = blob_service_client.get_container_client(container=digitalContainer)
    blobCurrent = input_container.get_blob_client("sendToPrM.trigger.txt")
    
    if blobCurrent.exists():
        logging.debug(f"Deleting bucket file: sendToPrM.trigger.txt")
        blobCurrent.delete_blob()
    
    input_container = blob_service_client.get_container_client(container=container_name)
    blob_list = input_container.list_blobs(name_starts_with=digital)

    if not os.path.exists(digitalDirectory): os.makedirs(digitalDirectory)
    
    for blob in blob_list:

        logging.info("[{}]:[INFO] : Blob name: {}".format(datetime.datetime.utcnow(), blob.name))
        #check if the path contains a folder structure, create the folder structure
        blob_name = setblobName(blob,digital)
        blob_client = input_container.get_blob_client(blob.name)
        dowlload_blob(blob_client,digitalDirectory+ "/"+blob_name)
    
    sendtoPrM(digitalDirectory,org_key,user,process_mining_api_key,stage2_config)

    blob_client = blob_service_client.get_blob_client(container="stage2", blob="deleteFiles.trigger.txt")
    input_stream = 'Trigger Delete'
    blob_client.upload_blob(input_stream, blob_type="BlockBlob")

    

@app.blob_trigger(arg_name="myblob", path="stage2/deleteFiles.trigger.txt",
                               connection="AzureWebJobsStorage") 
def deleteFiles(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, "stage2.yaml"), "r") as f:
        stage2_config = yaml.load(f, Loader=yaml.FullLoader)

    connection_string = os.environ['AzureWebJobsStorage']

    container_name = stage2_config['bucket-directories']['container']

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    input_container = blob_service_client.get_container_client(container=container_name)
    blob_list = input_container.list_blobs()

    for blob in blob_list:

        blob_client = input_container.get_blob_client(blob.name)
        blob_client.delete_blob()