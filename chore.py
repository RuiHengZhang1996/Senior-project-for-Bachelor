from __future__ import print_function
import boto3
from datetime import datetime, timedelta
import urllib
import re 



bucket_index = "ucd-senior-index"
bucket =s3.Bucket(bucket_index)
s3 = boto3.resource('s3')

def lambda_handler(event, context):
    
    #### Clean up actual file 
    config = lambda_config()
    back_up_path= config.get('back_up_path', "ucd-indexing/back_up")
    main_back_up_path = config.get('main_back_up_path', "ucd-indexing/main_back_up")
    main_path = config.get('main_path', "ucd-indexing" )
    error_file_path = config.get('error_file_path', "ucd-indexing/back_up/file_cannot_delete.txt")
    default_day = config.get('default_day', 30)
    past_date = current_date = (datetime.now() - timedelta(days = default_day))
    #### Clean up index 

    clean_backup(past_date,back_up_path, error_file_path)
    clean_backup(past_date,main_back_up_path, error_file_path)
    clean_backup(past_date, main_path, error_file_path)

def clean_up_actual_data(file_list):
    main_file_list = [ item for item in file_list if "back_up" not in item ]
    file_to_delete =[]
    for file_name in main_file_list:
        files = s3.Object(bucket_index, file_name).get()['Body'].read()
        data  = re.split("\n|\r", files)
        header = data[0].split(" - ")
        for item in data[1:]:
            file_to_delete.append(path_builder(header,item))
    return file_to_delete 

def path_builder(header, file_path):    
    if "&&" in file_path:
        file_path= file_path.replace("&&", header[2])
    if "part" not in file_path:
        tail = file_path.split(".")[-1]
        new_tail = "part" + tail + str(header[3])
        file_path = file_path[:-len(tail)] + new_tail

    return file_path

def clean_backup(past_date, path, error_file_path):

    objs = list(bucket.objects.filter(Prefix= path))
    file_to_delete = []
    for item in  objs:
        if "part" in item.key :
            file_date = (re.split("-index-|.part",item.key))[1]
            file_date = datetime.strptime(file_date, '%Y-%m-%d')
            # change back to <
            # keep this for testing
            file_to_delete.append(item.key) if file_date < past_date else ""
    # print file_to_delete
    if path == "ucd-indexing":
        file_to_delete = clean_up_actual_data(file_to_delete)
    delete_file(file_to_delete, error_file_path)
    print ("Done !")

def delete_file(file_list, error_file_path):
    try:
        for file in file_list:
            f = s3.Object(bucket_index, file)
            print ("Deleting %s" % file)
            f.delete() 
    except:
        f = s3.Object(bucket_index, error_file_path )
        data = data + file + "\n"
        f.put(Body = data )     
        pass 

def lambda_config():
    config_file = "ucd-indexing/lambda-config.txt"
    bucket_index = "ucd-senior-index"
    result = {}
    obj = s3.Object(bucket_index, config_file)
    for line in  obj.get()['Body'].read().split("\n"):
        line = line.split("=")
        result[line[0]] = line[1] if len(line) == 2 else None 
    return result

if __name__ == '__main__':

    lambda_handler(1,1)
    
    
