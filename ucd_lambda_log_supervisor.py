# from __future__ import print_function
from concurrent.futures import ThreadPoolExecutor
from Queue import Queue
import boto3
from datetime import datetime, timedelta
import re
import json
import math
from concurrent import futures
print('Loading function..')

# Assume all files located in logfilebucket123
# change it to "du-senior-ui-project" when move to CISCO Server 
s3 = boto3.resource('s3')
client = boto3.client('lambda', region_name='us-east-1')
bucket_name = 'ucd-lambda-storage'
bucket_index = 'ucd-lambda-index'
bucket =s3.Bucket(bucket_name)
return_result = Queue()
folder_path = "ucd-indexing"
num_worker = 20
max_num_worker = 49
max_file_len = 10000
cap_file_len = 2000
#### GOLDEN number
max_num_file_per_worker = 225

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
	 				file_in_dt_end = str(day_list[-1]) + ' ' + file_in_dt_end
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
		for log in file.get()['Body'].read().replace("}{", "}*^*{").split("*^*"):
			if (re.search(search_term, log, re.IGNORECASE)):
				log = log.replace("@timestamp", "1timestamp")
				return_result.put(log)
				print ("Search SUCCESS for File")
			
	except:
		print ("Search FAILURE for File")
		print file_name

def auto_scaling(file_len):
	num_file_per_worker = 120 
	num_worker = 40

	if file_len< 200 :
		num_worker = 2
	else :
		num_worker = int(math.ceil(file_len/float(num_file_per_worker)))
		if num_worker > 60:
			num_worker = 60 
			num_file_per_worker = int(math.ceil(file_len/60))

	num_file_per_worker =225 if num_file_per_worker > 225 else num_file_per_worker

	return num_worker, num_file_per_worker

# implement multi-cluster :D
def supervisor(file_list, search_term):
	result = []
	if len(file_list) > cap_file_len+1:
		result = file_list_splitter(file_list, cap_file_len)
		num_worker, num_file_per_worker = auto_scaling(len(result[0]))
		file_list = file_list_splitter(result[0], num_file_per_worker)

	num_worker, num_file_per_worker = auto_scaling(len(file_list))
	file_list = file_list_splitter(file_list, num_file_per_worker)

	payload = {}

	with ThreadPoolExecutor(max_workers = len(file_list)) as search_execute:
		for file in file_list:
			payload["file_list"] = file
			payload["search_term"] = search_term
			search_execute.submit(worker, payload)
	return result[1:] if result != None else None
	
def worker(payload):
	invoke_response = client.invoke(FunctionName="ucd-lambda-worker-0",
		                                    InvocationType='RequestResponse',
		                                    Payload= json.dumps(payload)
		                                    )

	if invoke_response['ResponseMetadata']['HTTPStatusCode'] == 200:
		return_result.put(json.loads(invoke_response['Payload'].read()))

def queue_flushing():
	while not return_result.empty():
	    return_result.get()
        

def file_list_splitter(file_list, num_file_per_worker):
	return [file_list[i:i+num_file_per_worker] for i in range(0, len(file_list), num_file_per_worker)]

def search_data(search_query,start,end):
	search_query = "message"
	start = "2018-02-24T07:59"
	end = "2018-02-24T12:59"

	current_time = datetime.now()
	start_utc = utc_time_converter(start, current_time)
	end_utc = utc_time_converter(end, current_time)

	# first, convert start,end date to list of dates
	date_list = process_date_filter(start_utc, end_utc)
	# Then create a list of possible files
	file_list = total_files_to_search(bucket, date_list,start_utc,end_utc)

	result = supervisor(file_list, search_query)
	final_result = list(return_result.queue)

	return final_result, result

def partial_search(file_list, search_term) :
	result = supervisor(file_list, search_query)
	return list(return_result.queue)


if __name__ == '__main__':

	result = main_function(1)

	final_result = list(return_result.queue)
	print result
	# return final_result, result 
	# error handling
	print final_result[2]
	if "timed out after" in final_result[0]:
		print "this is time out"
	if "body size is too long"  in final_result[0]:
		print "omg"
	print len(final_result)


