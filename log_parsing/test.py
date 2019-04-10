import os
import re
from collections import Counter
from LambdaLog-master import new_search

dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
path = dir_path + "/storage/" + "events" 

def extract_datetime_from_path(file_path):
	try:
		result = re.findall(r'.(\w+)-(\w+)-(\w+)T(\w+)', file_path)
		return ".".join(result[0][:-1]) if len(result)>0 else ""
	except : 
		print ("cant find correct date from given path")
		return ""


list_item = ["events", "logs"]
for item in file_type.split(","):
    item =new_search.get_date_list(item)
    print item

# print set(result)
