# from __future__ import print_function
from concurrent.futures import ThreadPoolExecutor
from Queue import Queue
import boto3
from datetime import datetime, timedelta
import re
import json
import math
print('Loading function..')

# Assume all files located in logfilebucket123
# change it to "du-senior-ui-project" when move to CISCO Server 
s3 = boto3.resource('s3')
bucket_name = 'du-senior-ui-project'
bucket_index = 'ucd-senior-index'
bucket =s3.Bucket(bucket_name)
return_result = Queue()
folder_path = "ucd-indexing"
# current_date = datetime.now().strftime("%Y-%m-%d")

# for testing 
current_date = (datetime.now() + timedelta(days =1)).strftime("%Y-%m-%d")
reg_file = "{}/main-index-reg-{}.txt".format(folder_path, current_date)
audit_file = "{}/main-index-audit-logs-{}.txt".format(folder_path, current_date)


# convert start date  and end date to a 
# list of dates between the two.
# start, end format : 2017-05-15T10:12
def process_date_filter(start_time, end_time):
	try:
		result =[]
		num_day = (end_time - start_time).days
		#### strip the hour, minutes, seconds
		while int(num_day+2) > 0:
			result.append(start_time.date())
			start_time += timedelta(days =1)
			num_day -=1
	except ValueError :
		print ("Fail at process_date_filter")
		return None
	return result

# time querry : 2017-10-07T17:20 format
# current time : just datetime.now() format 
# return utc time base on current time on local machine.
def utc_time_converter(time_querry, current_time):
	try:
		time = datetime.strptime(time_querry, '%Y-%m-%dT%H:%M')
		diff = abs(current_time - datetime.utcnow())
	except ValueError:
		print ("Fail to convert UTC time" + time_querry)
		return None

	return (time + timedelta(hours = diff.seconds/ 3600))

# Find all files need to be search for 
# bucket_name is = du-senior-ui-project
# day_list gets from filter 
# return a list of files to search
def total_files_to_search(bucket,day_list,start_utc, end_utc):
	result = []
	file_list = []

	#if there are no days to search
	if len(day_list) == 0:
		return []


	#looks into the index s3 bucket for indexes
	#returns a list of file paths for the day
	bucket = s3.Bucket(bucket_index)
	objs = list(bucket.objects.filter(Prefix=folder_path))
	for item in objs:
		if ("back_up" not in item.key) and ("main-index" in item.key):
			if len(day_list) > 0:
				for day in day_list[1:(len(day_list))]:
					#use full_index if the dates are in the past
					if (str(day) in item.key) and ("full_index" in item.key):
	 					file_list.append(item.key)
	 				#use daily index if the end date is today
	 				elif (str(day) in item.key) and (str(day) >= current_date):
	 					file_list.append(item.key)

	#grabs the files from the indexes
	for file_name in file_list:
		file = s3.Object(bucket_index, file_name)
		doc = file.get()['Body'].read()

		path = ''
		filetype = ''

	 	for line in re.split('\r|\n', doc):
	 		#get the common path
	 		#specified in each index file
	 		if '*END_SUMMARY*' in line:
	 			path = re.findall(r'- (\w+)(\.)(\w+)(\.)(\w+)(\/)(\w+)(\-)(\w+) -', line)
	 			if path:	
	 				path = "".join(path[0])
	 			if not path:
	 				path = ''
	 			filetype = re.findall(r'- (\.)(\w+) -', line)
	 			filetype = "".join(filetype[0])
	 		else:
	 			#for the first day in the list
	 			if str(day_list[0]) in line:
	 				#get the date and time from each file in the index
	 				file_in_dt_start = extract_datetime_from_path(str(line),str(day_list[0]))
	 				file_in_dt_start = str(day_list[0]) + ' ' + file_in_dt_start
	 				file_in_dt_start = datetime.strptime(file_in_dt_start, '%Y-%m-%d %H.%M')
	 				#compare first date and the start date
	 				if file_in_dt_start > start_utc and '&&' in line:
	 					#match the format for the path
	 					part = re.match('.*?([0-9]+)$', line).group(1)
	 					line = line + filetype
	 					line = line.replace('.' + part + filetype, '.part' + part + filetype)
	 					line = line.replace('&&', path)
	 					result.append(line)
	 				#if common path is not required
	 				elif file_in_dt_start > start_utc and '&&' not in line:
	 					result.append(line)
				#for the last day in the list
	 			if str(day_list[-1]) in line:
	 				file_in_dt_end = extract_datetime_from_path(str(line),str(day_list[-1]))
	 				file_in_dt_end = str(day_list[0]) + ' ' + file_in_dt_end
	 				file_in_dt_end = datetime.strptime(file_in_dt_end, '%Y-%m-%d %H.%M')
	 				#compare last date and the end date
	 				if file_in_dt_end < end_utc and '&&' in line:
	 					part = re.match('.*?([0-9]+)$', line).group(1)
	 					line = line + filetype
	 					line = line.replace('.' + part + filetype, '.part' + part + filetype)
	 					line = line.replace('&&', path)
	 					result.append(line)
	 				elif file_in_dt_end < end_utc and '&&' not in line:
	 					result.append(line)
	 			#for dates in between the first and last date
	 			for item in day_list[1:-1]:
	 				if str(item) in line:
	 					if '&&' in line:
	 						part = re.match('.*?([0-9]+)$', line).group(1)
	 						line = line + filetype
	 						line = line.replace('.' + part + filetype, '.part' + part + filetype)
	 						line = line.replace('&&', path)
	 						result.append(line)
	 					elif '&&' not in line:
	 						result.append(line)

	return result

# This is to extract the datetime from the path file
# converts to time
# only works for given format
def extract_datetime_from_path(file_path, date):
	try:
		result = re.findall(date + r'T' + r'(\w+)(\.)(\w+)', file_path)
	except e : 
		print ("cant find correct date from given path")
		return None
	return "".join(result[0])
    

# Given the bucket, which file to look at and search term
# return a list of log files that contain search_term
# ASUME CONDITION: each log is wrapped by {...}
# seach result start with a file_name where it get data from 
# follows with a list of return result.
def search_a_file(bucket,file_name,search_term):
	try:
		for log in file.get()['Body'].read().split("*^*"):
			if (re.search(search_term, log, re.IGNORECASE)):
				return_result.put(log)
				print ("Search SUCCESS for File")
			
	except:
		print ("Search FAILURE for File")
		print file_name
	
# implement multi thread 
def supervisor(file_list, search_term):
	final_result = Queue()
	num_file_list = len(file_list)
	max_num_file_per_worker = 200
	num = int(math.ceil(num_file_list)/max_num_file_per_worker)
	num_worker = 1 if num == 0 else num
	print num_worker, " num worker"

	with ThreadPoolExecutor(max_workers = num_worker) as search_execute:
		for file_object in file_list:
			# search_execute.submit(search_a_file, bucket_name, file_object, search_term)
			search_execute.submit(search_test, bucket_name, file_object, search_term)

def queue_flushing():
	while not return_result.empty():
	    return_result.get()
        
#### MAIN FUNCTION.
### Here we go ...
def main_function(event):
    # search_query = event['params']['querystring']['search_query']
    # start = event['params']['querystring']['start_date']
    # end = event['params']['querystring']['end_date']
    # current_time = event['params']['querystring']["current_time"]
    
    #### TEST EXAMPLE
    current_time = datetime.now()
    search_query = "message"
    start = "2018-02-24T07:59"
    end = "2018-02-25T12:59"
    
    #text = re.findall(r'(\w+)-(\w+)-(\w+)T(\w+).(\w+)', file_path)

    current_time = datetime.now()
    start_utc = utc_time_converter(start, current_time)
    end_utc = utc_time_converter(end, current_time)
    

    # first, convert start,end date to list of dates
    date_list = process_date_filter(start_utc, end_utc)
    # Then create a list of possible files
    file_list = total_files_to_search(bucket, date_list,start_utc,end_utc)
    # print len(file_list)
    # print file_list[100:120]
    # start searching
    supervisor(file_list[100:200], search_query)

    
def lambda_handler(event, context):
    s1 = datetime.now()
    # main_function(event)
    # e1 = datetime.now()
    # print ("this is time ", (e1 -s1).total_seconds())
    
    # final_result = list(return_result.queue)
    # queue_flushing()
    
    # print (len(final_result))
    # return (final_result)

def search_test(bucket,file_name,search_term):
	# try:
	result = Queue()
	file = s3.Object(bucket,file_name)
	
	for log in file.get()['Body'].read().split("*^*"):
		if (re.search(search_term, log, re.IGNORECASE)):
			return_result.put(log)
			print ("Search SUCCESS for File")
	# for log in file.get()['Body'].read().replace("}{", "}*^*{").split("*^*"):
	# 	log = log.replace("@timestamp", "1timestamp")
	# 	return_result.put(json.loads(log)) if (re.search(search_term, log, re.IGNORECASE)) else None
	# 	print ("Search SUCCESS for File")
	print list(result.queue)
	# except:
		# print ("Search FAILURE for File")
		# print file_name

if __name__ == '__main__':
	a = "ucd-indexing/main-index-2018-02-25.part1.txt"
	main_function(1)
	# file_list = ['logs/2018.02.24/huron-log-kafka-01-internal.na-sbox1.scplatform.cloud/ls.s3.bd74f2bf-d00d-472a-b2d1-04a5356afccc.2018-02-24T15.45.part63.txt', 'logs/2018.02.24/huron-log-kafka-01-internal.na-sbox1.scplatform.cloud/ls.s3.b77c2bd4-63e9-4321-8b4c-1699581021e0.2018-02-24T15.00.part60.txt', 'logs/2018.02.24/huron-log-kafka-01-internal.na-sbox1.scplatform.cloud/ls.s3.b5e600b3-ef8e-47fc-b28c-42a1f4458ca0.2018-02-24T15.30.part62.txt', 'logs/2018.02.24/huron-log-kafka-01-internal.na-sbox1.scplatform.cloud/ls.s3.add67360-e83e-43b5-af99-22b11b94d642.2018-02-24T15.22.part59.txt', 'logs/2018.02.24/huron-log-kafka-01-internal.na-sbox1.scplatform.cloud/ls.s3.87a43e70-d6ca-487e-bd10-b2a9f0ea0eea.2018-02-24T15.16.part61.txt', 'logs/2018.02.24/huron-log-kafka-01-internal.na-sbox1.scplatform.cloud/ls.s3.7de35331-271e-423d-a12e-a69384979152.2018-02-24T15.06.part58.txt', 'logs/2018.02.24/huron-log-kafka-01-internal.na-sbox1.scplatform.cloud/ls.s3.7c2d8fe4-6c28-4abc-bd13-302090259b37.2018-02-24T15.31.part62.txt', 'logs/2018.02.24/huron-log-kafka-01-internal.na-sbox1.scplatform.cloud/ls.s3.54bb9a94-b076-4653-ac0b-eadf104dfd4a.2018-02-24T15.46.part63.txt', 'logs/2018.02.24/huron-log-kafka-01-internal.na-sbox1.scplatform.cloud/ls.s3.45670b46-b1ac-4388-b0c7-618aa17cc0a3.2018-02-24T15.14.part59.txt', 'logs/2018.02.24/huron-log-kafka-01-internal.na-sbox1.scplatform.cloud/ls.s3.1e2ea6ee-f9d1-4b11-86e0-19b5cb7c8c50.2018-02-24T15.01.part60.txt', 'logs/2018.02.24/huron-log-kafka-01-internal.na-sbox1.scplatform.cloud/ls.s3.1db16a27-b03c-4b01-94ad-2da23f91b7db.2018-02-24T15.30.part60.txt', 'logs/2018.02.24/huron-log-kafka-01-internal.na-sbox1.scplatform.cloud/ls.s3.0ca949a6-6dd7-4fb6-9651-a6661c919353.2018-02-24T15.54.part61.txt', 'logs/2018.02.24/huron-log-cf-x-router-01-internal.na-sbox1.scplatform.cloud/ls.s3.4f5cbff9-e081-4123-a847-681bbeed2ada.2018-02-24T15.17.part0.txt', 'logs/2018.02.24/huron-log-cf-x-router-01-internal.na-sbox1.scplatform.cloud/ls.s3.2c42e037-6a47-4158-97f3-a2b2b19b8629.2018-02-24T15.33.part0.txt', 'logs/2018.02.24/huron-log-cf-x-router-01-internal.na-sbox1.scplatform.cloud/ls.s3.21b25c57-7e29-4a3b-ad92-2758ed6d04cb.2018-02-24T15.33.part15.txt', 'logs/2018.02.24/huron-log-cf-x-router-01-internal.na-sbox1.scplatform.cloud/ls.s3.1c7e1180-412e-404e-a1f5-de6956e16089.2018-02-24T15.17.part0.txt', 'logs/2018.02.24/huron-log-cf-x-nats-02-internal.na-sbox1.scplatform.cloud/ls.s3.e069da7b-886a-4a4f-a9b8-d002688301c3.2018-02-24T15.28.part10.txt', 'logs/2018.02.24/huron-log-cf-x-nats-02-internal.na-sbox1.scplatform.cloud/ls.s3.a71b4d2a-9d45-46e3-b06f-533d491a132f.2018-02-24T15.17.part0.txt', 'logs/2018.02.24/huron-log-cf-x-nats-02-internal.na-sbox1.scplatform.cloud/ls.s3.42759340-22cc-4c78-9ca6-6447bb344b6c.2018-02-24T15.17.part0.txt', 'logs/2018.02.24/huron-log-cf-x-nats-01-internal.na-sbox1.scplatform.cloud/ls.s3.d3b9480d-c235-4f33-99b1-b450af6d3453.2018-02-24T15.51.part0.txt']
	# for file in file_list:
		search_test(bucket_name,file,"message")


