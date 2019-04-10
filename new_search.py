import os, fnmatch
import re
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter

config_file ="config.csv"
def load_config(config_file):
	config ={}
	file = open(config_file, "r")
	for item in file:
		if len(item):
			temp = item.replace("\n", "").split("=") 
			config[temp[0]] = temp[1]
	return config

config = load_config(config_file)

# here to pull when needed
log_file      = config.get("log_file" ,"log_message.log")
storage_path  = config.get("storage_path", "/storage")
index_file    = config.get("index_file", "index.csv")
storage_size  = config.get("storage_size", 0)
storage_atime = config.get("storage_atime", 0) 
storage_mtime = config.get("storage_mtime", 0)
storage_ctime = config.get("storage_ctime", 0)
admin_email   = config.get("admin_email", "")


def _find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result

def write_to_file(file_path, message):
	if os.path.isfile(file_path):
		file = open(log_file, "a")
	else:
		file = open(log_file, "w")
	current_time = datetime.now().strftime("%Y-%m-%d %H:%m:%s")
	file.write(current_time + ":  " + message + "\n")
	file.close()


def search_in_file(file_path,search_term):
	result = []
	#### get base path and remove \n char 
	dir_path = os.path.dirname(os.path.realpath(__file__))
	file = open( dir_path + file_path.replace("\n", ""), "r")
	try:
		file = open( dir_path + file_path.replace("\n", ""), "r")
		for chunk in file:
			for log in str(chunk).replace("}{", "}*^*{").split("*^*"):
				result.append(log) if (re.search(search_term, log, re.IGNORECASE)) else None		
	except:
		#### log fail search action 
		message = "{} does not exist".format(file_path)
		write_to_file(log_file, message)
		print message	
	return result

def supervisor(file_list, search_term):
	result = []
	for file in file_list:
		if len(result) != None:
			temp_result = search_in_file(file, search_term)
			result = (result + temp_result  )

	return result

### hash file
def file_compare(new_file):
	stat = os.stat(new_file)
	if  (stat.st_size  != storage_size  ) or \
		(stat.st_atime != storage_atime ) or \
		(stat.st_mtime != storage_mtime ) or \
		(stat.st_ctime != storage_ctime) :
		return True
	return False

def indexing():
	try:		
		dir_path = os.path.dirname(os.path.realpath(__file__))
		path = dir_path+ storage_path
		if file_compare(path):
			file_list =  _find('*.txt', path)
			file = open(index_file, "w")
			for item in file_list:
				item = item.replace(dir_path, "")
				file.write(item + "\n")
			file.close()
			#### update config file with new hash 
			message = "Update new index file !!!"
			write_to_file(log_file,message)
			print message
	except:
		message = " Fail to index"
		write_to_file(log_file,message)
		print message

def extract_datetime_from_path(file_path):
	try:
		result = re.findall(r'.(\w+)-(\w+)-(\w+)T(\w+)', file_path)
		return "-".join(result[0][:-1]) if len(result)>0 else ""
	except : 
		print ("cant find correct date from given path")
		return ""

def get_date_list(folder):
	result = []
	dir_path = os.path.dirname(os.path.realpath(__file__))
	path = dir_path + "/storage/" + folder
	result = []
	final_result = {}
	for root, dirs, files in os.walk(path):
	    for name in files:
	    	result.append(extract_datetime_from_path(name))

	list_key =Counter(result).values()
	for index, val in enumerate(Counter(result).keys()):
		final_result[val] =  list_key[index] 

	return final_result

def process_date_filter(start, end):
	try:
		result = []
		start_date = datetime.strptime(start, '%m/%d/%Y %H:%M')
		end_date = datetime.strptime(end, '%m/%d/%Y %H:%M')
		num_day = (end_date - start_date).days
		while int(num_day + 2) > 0:
			result.append(start_date.date())
			start_date += timedelta(days = 1)
			num_day -= 1
	except ValueError:
		print ("Failed at process_date_filter")
		return None
	return result

def poke_file(day_list):
	result = []
	file = open(index_file, "r")
	for file_name in file:
		for item in day_list[0:-1]:
			#Add to result list if the date is in the file name
			if str(item) in file_name:
				result.append(file_name)
	return result

if __name__ == '__main__':
	# a = ["ab","bd", "fd"]
	# if "bd" in ["ab","bd", "fd"] :
	# 	print "Yes"
		
	# day_list= process_date_filter('02/21/2018 00:00', '02/25/2018 00:00')
	# print poke_file(day_list)
	indexing()
	# # # main()
	# dir_path = os.path.dirname(os.path.realpath(__file__))
	# dir1 = dir_path + "/storage"
	# # path =  dir_path + "/storage/events/2018.01.19/huron-event-127.0.0.1/ls.s3.7738c9ed-007d-44e1-88fd-7ae04e736654.2018-01-19T01.09.part4.txt"
	# search_term = "index"
	# result = search_in_file(path,search_term)
	# print result

	# file_list =  find('*.txt', dir1)
	# a = "posix.stat_result(st_mode=16877, st_ino=1637583, st_dev=16777218, st_nlink=5, st_uid=769544919, st_gid=1745984435, st_size=170, st_atime=1521091416, st_mtime=1521056045, st_ctime=1521081319)"
	# # file = open("index.csv", "w")
	# b = "posix.stat_result(st_mode=16877, st_ino=1637583, st_dev=16777218, st_nlink=6, st_uid=769544919, st_gid=1745984435, st_size=204, st_atime=1521091613, st_mtime=1521091609, st_ctime=1521091609)"
	# c =  os.stat(dir_path + "/storage")
	# print c.st_mtime



	# for item in file_list:
	# 	item = item.replace(dir_path, "")
	# 	file.write(item + "\n")
	# file.close()


	# file_list = []
	# file = open("index.csv", "r")
	# for item in file:
	# 	file_list.append(item)

	# a = supervisor(file_list[:10], "message")
	# print a


