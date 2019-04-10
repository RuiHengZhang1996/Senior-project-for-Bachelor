# from __future__ import print_function
import boto3
from datetime import datetime, timedelta
import urllib
from concurrent.futures import ThreadPoolExecutor


s3 = boto3.resource('s3')
bucket_index = "ucd-lambda-index"
config = lambda_config()

folder_path = config.get('folder_path', "ucd-indexing")
back_up_path = config.get('back_up_path',"ucd-indexing/back_up")
main_back_up_path = config.get('main_back_up_path', "ucd-indexing/main_back_up")
max_size = config.get( 'max_size' , 1000000) #1MB
medium_size = config.get('medium_size', 500000) #500kb
min_size =  config.get( 'min_size' ,100000 )# 100kb


def lambda_handler(event, context):
	supersisor()

def lambda_config():
    config_file = "ucd-indexing/lambda-config.txt"
    bucket_index = "ucd-lambda-index"
    result = {}
    obj = s3.Object(bucket_index, config_file)
    for line in  obj.get()['Body'].read().split("\n"):
        line = line.split("=")
        result[line[0]] = line[1] if len(line) == 2 else None 
    return result

def supersisor():
	# current_date = datetime.now().strftime("%Y-%m-%d")
	# yesterday = (datetime.now() - timedelta(1)).strftime("%Y-%m-%d")
	current_date = (datetime.now() - timedelta(2)).strftime("%Y-%m-%d")
	yesterday = (datetime.now() - timedelta(3)).strftime("%Y-%m-%d")
	# test val


	bucket = s3.Bucket(bucket_index)
	objs = list(bucket.objects.filter(Prefix=folder_path))
	today_file = []
	yesterday_file = []
	main_file = []
	yesterday_main_file = []
	left_over = []

	for item in  objs:
		if "back_up" not in item.key:
			today_file.append(item.key) if "daily-index-{}".format(current_date) in item.key else ""
			yesterday_file.append(item.key) if "daily-index-{}".format(yesterday) in item.key else ""
			main_file.append(item.key) if "main-index-{}".format(current_date) in item.key else ""
			yesterday_main_file.append(item.key) if "main-index-{}".format(yesterday) in item.key else ""
	
	# chunk = auto_scaling(today_file+ yesterday_file)
	# testing val
	chunk = 4

	main_list, _ =chunk_divider(today_file, chunk)
	merge_file(main_list, main_file,current_date, chunk)
	### back up and delete 
	copy_file(main_list)
	delete_file(main_list)
	
	if len(yesterday_main_file) > 0 :
		yesterday_list, temp = chunk_divider(yesterday_file, chunk)
		left_over.append(temp) if len(temp) >0 else ""
		merge_file(yesterday_list, yesterday_main_file,yesterday, chunk)
		copy_file(yesterday_list)
		delete_file(yesterday_list)

		if len(left_over)>0:
			merge_file(left_over, yesterday_main_file,yesterday,chunk)
			copy_file(left_over)
			delete_file(left_over)

def delete_file(file_list):
	for files in file_list:
		for file in files:
			f = s3.Object(bucket_index, file)
			f.delete()

def merge_file(file_list,main_file,date, chunk):
	total_num_file = 24/chunk
	num = int(get_current_file_part(main_file))

	current_part = num + 1 if num <= total_num_file else 0
	for item in file_list:
		body = merge_data(item)
		temp_index_file = "{}/main-index-{}.part{}.txt".format(folder_path, date ,current_part)
		file = s3.Object(bucket_index, temp_index_file)
		file.put(Body = body)
		back_up_single_file(temp_index_file)
		current_part = current_part + 1
	print ("Done !")

def back_up_single_file(file):
	des_path = file.replace(folder_path, main_back_up_path)
	copy_source = {'Bucket': bucket_index ,'Key': file }
	bucket = s3.Bucket(bucket_index)
	bucket.copy(copy_source, des_path)

def get_current_file_part(main_file):
	max_val = 0
	for item in main_file:
		temp =  int(item.split(".part")[1].split(".txt")[0])
		max_val = temp if temp > max_val else max_val
	return max_val

def chunk_divider(today_file, chunk):
	today_file_dict = {}
	
	for item in today_file:
		today_file_dict[item] = int(item.split(".part")[1].split(".txt")[0])
	count = 0 
	temp_list = []
	main_list = []

	for key, value in sorted(today_file_dict.iteritems(), key=lambda (k,v): (v,k)):
		if count < chunk-1:
			temp_list.append(key)
			count = count +1 
		else :
			main_list.append(temp_list)
			temp_list.append(key)
			temp_list = []
			count = 0
	return main_list, temp_list

def copy_file(file_list):
	for files in file_list:
		for file in files:
			des_path = file.replace(folder_path, back_up_path)
			copy_source = {'Bucket': bucket_index ,'Key': file }
			bucket = s3.Bucket(bucket_index)
			bucket.copy(copy_source, des_path)

def merge_data(file_list):
	file = []
	data = ""
	for item in file_list:
			file.append(s3.Object(bucket_index, item))
	main_body, main_header = file_parser(file[0])
	for item in file[1:]:
		body, header = file_parser(item)
		main_body,main_header = build_data(main_body, main_header, body, header)

	new_header = " - ".join(main_header)+ " -  *END_SUMMARY*"
	return (new_header + main_body)

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

def file_parser(file):
	result = file.get()['Body'].read().split("*END_SUMMARY*")
	header = result[0].split(" - ")
	return result[1], header

def auto_scaling(file_list):
	file = []
	for item in file_list:
		file.append(s3.Object(bucket_index, item).get()['ContentLength'])
	val = median(file)
	if val > max_size:
		return 24
	elif val > medium_size and val < max_size:
		return 8
	elif val > min_size and val < medium_size:
		return 4
	elif val < min_size:
		return 1 
	else:
		return 4

def median(item_list):
	pass

if __name__ == '__main__':

	a = ['1751153B', '69', '2018.02.25/huron', '.txt', ' ']
	b = ['1752224B', '31', '2018.02.25/huron', '.txt', ' ']
	
	supersisor()
	# build_data("1", a, "1", b )
	# bucket = s3.Bucket(bucket_name)
	# objs = list(bucket.objects.filter(Prefix=folder_path))
	# file_list = []
	# for item in  objs:
	# 	file_list.append(item.key) if "daily-index-2018-02-25" in item.key else ""

	# # print file_list
	# # print merge_file(file_list[:4])
	# print auto_scaling(file_list[:4])



	# copy_file( "ucd-indexing/daily-index-2018-02-10-3.txt" ,"ucd-indexing/test_copy file")

	# print (data)
	# a = test3()
