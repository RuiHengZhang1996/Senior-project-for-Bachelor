# from __future__ import print_function
from __future__ import print_function
# from __future__ import print_function
import boto3
from datetime import datetime, timedelta
import urllib
import re


s3 = boto3.resource('s3')
part = "part"
def lambda_handler(event, context):  
    # try:
        config = lambda_config()
        folder_path = config.get('folder_path', 'ucd-indexing')
        bucket_index =config.get('bucket_index', 'ucd-senior-index')
        bucket_name = config.get('bucket_name', 'du-senior-ui-project')
        bucket =s3.Bucket(bucket_index)
    
        current_date = datetime.now().strftime("%Y-%m-%d")
        temp_index_file = "{}/daily-index-{}.part{}.txt".format(folder_path, current_date,datetime.now().hour)
        
        file_path = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
        file_size = (event['Records'][0]['s3']['object']['size'])
        clean_up(bucket_name, file_path)
        data = ""
    
        # open the current daily file 
        file = s3.Object(bucket_index, temp_index_file )
        objs = list(bucket.objects.filter(Prefix=temp_index_file))
        # check whether the current file exist
        if len(objs) > 0 and objs[0].key == temp_index_file: 
            data = add_data_to_exist_file(file, file_path, file_size)
        else:
            # create new file
            file_type = file_path.split(".")[-1]
            data = str(file_size)+ "B" + " - " + "1" + " - " + "" + " - ." + file_type + " - " + " *END_SUMMARY* \n"
            data = data + file_path + "\n"
    
        date_checker(file_path)
    # write to file
        file.put(Body=data)

    # NOTIFY using email
        print ("Complete indexing new file !")
    # except:
    #     print ("fail to process single file indexing")

def clean_up(bucket, file_path):
    file = s3.Object(bucket,file_path)
    data = file.get()['Body'].read().replace("}{", "}*^*{")
    data = data.replace("@timestamp", "1timestamp")
    file.put(Body= data)
    
def add_data_to_exist_file(file,file_path ,file_size):
    try:
        doc = file.get()['Body'].read().split("\n")
        header = doc[0].split(" - ")
        first_file = doc[1]
    
        total_size = int(header[0][:-1]) + int(file_size)
        total_file = int(header[1]) + 1
        common_path = header[2]
        file_type = header[3]
    
        file_path, common_path = path_clean_up(file_path,common_path,file_type, first_file)
    
        # # # copy back to new string holder
        data = str(total_size)+ "B" + " - " + str(total_file) + " - " + common_path + " - " + file_type + " - " + " *END_SUMMARY* \n"
        data = data + first_file + "\n"
        data = data + file_path + "\n"
        
        temp = "\r".join(doc[2:])
        data = data + temp
        return data
    except:
        pass
        return ""

def path_clean_up(file_path,common_path,file_type,first_file):
    if (len(common_path) == 0) or (common_path not in file_path):
        common_path = common_path_finder(first_file,file_path)
    file_path = file_path.replace(common_path, "&&")
    # strip off file type
    if file_type in file_path:
        file_path = file_path.replace(file_type, "")
    if part in file_path:
        file_path = file_path.replace(part, "")
        
    return file_path, common_path

#### if path file does not have date. 
#### notify user but still let it pass. 
#### might store them seperate 
def date_checker(file_path):
    day_list = []
    day_list.append((datetime.now() + timedelta(days =1)).strftime("%Y.%m.%d"))
    day_list.append((datetime.now() + timedelta(days =1)).strftime("%Y-%m-%d"))
    day_list.append((datetime.now()).strftime("%Y.%m.%d"))
    day_list.append((datetime.now()).strftime("%Y-%m-%d"))
    day_list.append((datetime.now() + timedelta(days = -1)).strftime("%Y.%m.%d"))
    day_list.append((datetime.now() + timedelta(days = -1)).strftime("%Y-%m-%d"))
    if not any(date in file_path for date in day_list):
        print ("%s does not have date in file path or it is an old file " % file_path)

def common_path_finder(original,new):
    original = original.split("/",1)
    new = new.split("/",1)
    loop_len = len(original[1]) if len(original[1]) < len(new[1]) else len(new[1])
    
    for char in range (loop_len):
        if original[1][char] != new[1][char]:
            return original[1][:(char -1)]
    return 0

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
    lambda_config()
    # lambda_handler(1,1)
    # date_checker("this/is/the/file/2018.03.11/file_name2018-03-11.txt")
    a = "123logs/2018.02.19/huron-log-redis3-02-internal.na-sbox1.scplatform.cloud/ls.s3.f52003a5-3620-4497-86fd-0be503f544f2.2018-02-20T23.44.part94.txt"
    # b = "main-index-logs-2018-02-19.txt"
    # c ="main-index-reg-2018-02-20.txt"
    d = "daily-index-2018-02-25.part3.txt"
    # file = s3.Object(bucket_index, "ucd-indexing/{}".format(d) )
    # print file.get()['ContentLength']
    # print add_data_to_exist_file(file,a, 4000)
    # bb = path_clean_up(a,"huron-log",".txt",a)
    # print bb
    # cc = bb[0].split(".", -1)
    # new_term = "part" + cc[-1]
    # aaa =bb[0].replace(cc[-1], new_term)
    # print aaa 
    # print "part" + bb
    # print bb[0] +  part + cc 



