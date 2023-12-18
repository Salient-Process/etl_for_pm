import logging
import datetime

def dowlload_blob(blob_client, destination_file):
        logging.info("[{}]:[INFO] : Downloading {} ...".format(datetime.datetime.utcnow(),destination_file))
        with open(destination_file, "wb") as my_blob:
            blob_data = blob_client.download_blob()
            blob_data.readinto(my_blob)
        logging.info("[{}]:[INFO] : download finished".format(datetime.datetime.utcnow()))    

def setblobName(blob,valueToReplace):
       blob_name = blob.name.replace(valueToReplace+'/','')
       return blob_name