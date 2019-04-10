from concurrent.futures import ThreadPoolExecutor
from Queue import Queue
import boto3
from datetime import datetime, timedelta
import re
import json
import math


s3 = boto3.resource('s3')
bucket_name = 'du-senior-ui-project'
bucket_index = 'ucd-senior-index'
return_result = Queue()
result_file = "ucd-indexing/search_result.txt"

def search_a_file(bucket,file_name,search_term):
	try:
		file = s3.Object(bucket,file_name) 
		for log in file.get()['Body'].read().replace("}{", "}*^*{").split("*^*"):
			if (re.search(search_term, log, re.IGNORECASE)):
				log = log.replace("@timestamp", "1timestamp")
				return_result.put(json.loads(log))
				# print ("Search SUCCESS for File")
			
	except:
		print ("Search FAILURE for File")
		print file_name

def supervisor(file_list, search_term):
	final_result = Queue()
	num_file_list = len(file_list)
	max_num_file_per_worker = 20
	num = int(math.ceil(num_file_list)/max_num_file_per_worker)
	num_worker = 1 if num == 0 else num

	with ThreadPoolExecutor(max_workers = num_worker) as search_execute:
		for file in file_list:
			search_execute.submit(search_a_file, bucket_name, file, search_term)

def queue_flushing():
	while not return_result.empty():
	    return_result.get()

def lambda_handler(event, context):
	supervisor(event["file_list"], event["search_term"])
	final_result = list(return_result.queue)
	print len(final_result), "This is still okay."
	queue_flushing()

	return (final_result)


if __name__ == '__main__':
	def lambda_handler(1, 1)

