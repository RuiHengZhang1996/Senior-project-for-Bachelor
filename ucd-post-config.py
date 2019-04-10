
import json 
import boto3

##### POST #####
def lambda_handler(event, context):
	try:
	    s3 = boto3.resource('s3')
	    bucket_index = "ucd-senior-index"
	    config_file  = "ucd-indexing/lambda-config1.txt"
	    file = s3.Object(bucket_index, config_file )
	    data = {}
	    
	    data['bucket_name'] = event['params']['querystring']['bucket_name']
	    data['bucket_index'] = event['params']['querystring']['bucket_index']
	    data['folder_path'] = event['params']['querystring']['folder_path']
	    data['back_up_path'] = event['params']['querystring']['back_up_path']
	    data['main_back_up_path'] = event['params']['querystring']['main_back_up_path']
	    data['main_path'] = event['params']['querystring']['main_path']
	    data['error_file_path'] = event['params']['querystring']['error_file_path']
	    data['default_day'] = event['params']['querystring']['default_day']
	    data['max_size'] = event['params']['querystring']['max_size']
	    data['medium_size'] = event['params']['querystring']['medium_size']
	    data['min_size'] = event['params']['querystring']['min_size']
	    data['chunk'] = event['params']['querystring']['chunk']
	    
	    s = ""
	    for key, val in data.iteritems():
	    	s = s + key + "=" + val + "\n"
	    	file.put(Body= s)
	
	except:
		print "fail to update"


##### GET ######

def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    config_file = "ucd-indexing/lambda-config.txt"
    bucket_index = "ucd-senior-index"
    result = {}
    obj = s3.Object(bucket_index, config_file)
    for line in  obj.get()['Body'].read().split("\n"):
        line = line.split("=")
        result[line[0]] = line[1] if len(line) == 2 else None 
    return result


if __name__ == '__main__':
	main()



	
