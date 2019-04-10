# from __future__ import print_function
import boto3
from datetime import datetime, timedelta
import urllib
from concurrent.futures import ThreadPoolExecutor
import re
import os
import math 

s3 = boto3.resource('s3')
bucket_index = "ucd-senior-index"

def lambda_config():
    config_file = "ucd-indexing/lambda-config.txt"
    bucket_index = "ucd-senior-index"
    result = {}
    obj = s3.Object(bucket_index, config_file)
    for line in  obj.get()['Body'].read().split("\n"):
        line = line.split("=")
        result[line[0]] = line[1] if len(line) == 2 else None 
    return result


config = lambda_config()
chunk = config.get('chunk', 4)
bucket_name = config.get('bucket_name', "ucd-lambda-storage" )
folder_path = config.get('folder_path', "ucd-indexing")
back_up_path = config.get('back_up_path',"ucd-indexing/back_up")
main_back_up_path = config.get('main_back_up_path', "ucd-indexing/main_back_up")
chunk = 24

file_list = [ 'events', 'logs']
part = "part"




def extract_datetime_from_path(file_path):
	try:
		result = re.findall(r'.(\w+)-(\w+)-(\w+)T(\w+)', file_path)
		if len(result)>0 :

			return "-".join(result[0][:-1]), int(result[0][-1])
	except : 
		print ("cant find correct date from given path")
		return " "

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

def build_header(file_path, file_size, total_file):

	file_type = file_path.split(".")[-1]
	total_size = int(file_size)
	total_file += 1
	data = str(total_size)+ "B" + " - " + str(total_file) + " - " + "" + " - ." + file_type + " - " + " *END_SUMMARY* \n"
	return  data + file_path + "\n"


def common_path_finder(original,new):
    original = original.split("/",1)
    new = new.split("/",1)
    loop_len = len(original[1]) if len(original[1]) < len(new[1]) else len(new[1])
    
    for char in range (loop_len):
        if original[1][char] != new[1][char]:
            return original[1][:(char -1)]
    return 0

# write items to file 
def read_file_path(folder):
	current_date = datetime.now().strftime("%Y-%m-%d")
	client = boto3.client('s3')
	paginator = client.get_paginator('list_objects_v2')
	page_iterator = paginator.paginate(Bucket= bucket_name, Prefix = folder)
	list_data = [""] *24
	data = ''
	file_date = ''
	file_path = ''
	file_type = ''
	file_time = 0 
	total_file = 0
	total_size = 0
	for page in page_iterator.result_key_iters():
		for result in page:
			file_path = result["Key"]
			if len(file_path) > 30:
				# new file
				file_date, file_time = extract_datetime_from_path(str(file_path))
				if total_file == 0 :
					first_file_date =file_date
					list_data[file_time] = build_header(file_path, result["Size"], total_file)
					total_file +=1
				# if same day then check for time 
				else:
					if first_file_date in file_path:
						# same time
						if len(list_data[file_time]) > 0 :
							# concat data 
							list_data[file_time] = concat_data(list_data[file_time],file_path, result["Size"])
							total_file += 1
						else : 
							# new file
							list_data[file_time] = build_header(file_path, result["Size"], total_file)
					else:
						for index, item in enumerate(list_data):
							if len(item) > 0 : 
								temp_index_file = "{}/main-index-{}.part{}.txt".format(folder_path, file_date ,index)
								file = s3.Object(bucket_index, temp_index_file)
								file.put(Body = list_data[index])

						#### Clear all data 
						file_date, file_time = extract_datetime_from_path(str(file_path))
						first_file_date =file_date
						list_data[file_time] = build_header(file_path, result["Size"], 1)
						total_file = 0 
						list_data = [""] *24

	print ("successfully full-indexing %s" % folder)

def extract_date_from_path(file_path):
	try:
		result = re.findall(r'-(\w+)-(\w+)-(\w+).part', file_path)
		if len(result)>0 :

			return "-".join(result[0])
	except : 
		print ("cant find correct date from given path")
		return " "

def supervisor(folder_path):
	bucket = s3.Bucket(bucket_index)
	objs = list(bucket.objects.filter(Prefix=folder_path)) 
	temp = []
	print len(objs)
	current_date =  extract_date_from_path(objs[0].key)

	####  [1:] 
	for item in objs[1:]:
		if current_date in item.key :
			temp.append(item.key)
		else:
			file_list, incomplete_list = chunk_divider(temp,chunk)
			merge_file(file_list,current_date,chunk)
			# delete_file(file_list)
			if len(incomplete_list[0]):
				merge_file(incomplete_list,current_date,chunk)
				# delete_file(incomplete_list)

			temp = []
			current_date =  extract_date_from_path(item.key)
			temp.append(item.key)

	file_list, incomplete_list = chunk_divider(temp,chunk)
	merge_file(file_list,current_date,chunk)
	# delete_file(file_list)
	if len(incomplete_list[0]):
		merge_file(incomplete_list,current_date,chunk)
		# delete_file(incomplete_list)

def delete_file(file_list):
	for files in file_list:
		for file in files:
			f = s3.Object(bucket_index, file)
			f.delete()

def chunk_divider(today_file, chunk):
	today_file_dict = {}
	
	for item in today_file:
		today_file_dict[item] = int(item.split(".part")[1].split(".txt")[0])
	count = 0 
	temp_list = []
	main_list = []
	incomplete_list = []
	for key, value in sorted(today_file_dict.iteritems(), key=lambda (k,v): (v,k)):
		if count < chunk-1:
			temp_list.append(key)
			count = count +1 
		else :
			main_list.append(temp_list)
			temp_list.append(key)
			temp_list = []
			count = 0
	incomplete_list.append(temp_list)
	return main_list, incomplete_list

def merge_file(file_list,date, chunk):
	for files in file_list:
		body = merge_data(files)
		part = path_num_decider(files,chunk)
		temp_index_file = "ucd-indexing/main-index-{}.part{}.txt".format(date ,part)
		file = s3.Object(bucket_index, temp_index_file)
		file.put(Body = body)
	print ("Done !")

def merge_data(file_list):
	file = []
	data = ""
	for item in file_list:
		file.append(s3.Object(bucket_index, item))
	main_body, main_header = file_parser(file[0])
	for item in file[1:]:
		body, header = file_parser(item)
		main_body,main_header = build_data(main_body, main_header, body, header)
		#### Fix the common path
	new_header = " - ".join(main_header)+ " -  *END_SUMMARY*"
	return (new_header + main_body)

def file_parser(file):
	result = file.get()['Body'].read().split("*END_SUMMARY*")
	header = result[0].split(" - ")
	return result[1], header

def build_data(main_body, main_header, body, header):
	new_header = []
	common_path = ""
	if len(main_header[2]) < len(header[2]):
		common_path = str(main_header[2])
		extra_path = header[2].replace(common_path, "")
		body = body.replace("&&", "&&"+ extra_path)

	elif len(main_header[2]) > len(header[2]):
		common_path = str(header[2])
		extra_path = main_header[2].replace(common_path, "")
		main_body = main_body.replace("&&", "&&"+ extra_path)

	new_header.append(str(int(main_header[0][:-1]) + int(header[0][:-1])) + "B")
	new_header.append(str(int(main_header[1]) + int(header[1])))
	new_header.append(common_path)
	new_header.append(main_header[3])
	return (body + main_body) , new_header

def concat_data(data, file_path, file_size):   
	data = re.split("\n|\r", data)
	
	header = data[0].split(" - ")
	first_file = data[1]

	total_size = int(header[0][:-1]) + int(file_size)
	total_file = int(header[1]) + 1
	common_path = header[2]
	file_type = header[3]
	file_path, common_path = path_clean_up(file_path,common_path,file_type, first_file)

	# # # copy back to new string holder
	result = str(total_size)+ "B" + " - " + str(total_file) + " - " + common_path + " - " + file_type + " - " + " *END_SUMMARY* \n"
	result = result + first_file + "\n"
	result = result + file_path + "\n"
	temp = "\r".join(data[2:])
	result = result + temp
	return result

    # except:
    #     pass
    #     return ""

def path_num_decider(file_list,chunk):
	part = 0
	for file in file_list:
		temp = re.findall(r'(\w+).part(\w+).', file)
		part = int(temp[0][1]) if int(temp[0][1]) > part else part
	return int(math.ceil(part/float(chunk)))

def lambda_handler(event, context):
	with ThreadPoolExecutor(max_workers = len(file_list)) as search_execute:
		for item in file_list:
			future = search_execute.submit(read_file_path,item)
	folder_path = "ucd-indexing/full_index"
	supervisor(folder_path)

def reindex_from_backup():
	folder_path = "ucd-indexing/full_index"
	supervisor(folder_path)

def file_clean_up():
	folder_path = "logs"
	bucket = s3.Bucket(bucket_name)
	objs = list(bucket.objects.filter(Prefix=folder_path))

	with ThreadPoolExecutor(max_workers = 200) as search_execute:
		for file in objs:
			search_execute.submit(clean_up(bucket_name,file.key))



def clean_up(bucket, file_path):
    file = s3.Object(bucket,file_path)
    data = file.get()['Body'].read().replace("}{", "}*^*{")
    data = data.replace("@timestamp", "1timestamp")
    print file_path
    file.put(Body= data)

if __name__ == '__main__':
	# file_clean_up()
	# lambda_handler(1,1)
	# date_checker()
	# read_file_path(file_list[0])
	# supervisor()	
	# lambda_handler(1,1)
	# reindex_from_backup()

