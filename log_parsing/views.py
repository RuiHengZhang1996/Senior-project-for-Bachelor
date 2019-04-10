from django.shortcuts import render
from django.template import RequestContext
from django.contrib.auth.decorators import login_required
from django.contrib.auth import authenticate, login, logout
from django.shortcuts import render_to_response
from django.http import HttpResponse, HttpResponseRedirect
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger

from graphos.sources.simple import SimpleDataSource
from graphos.renderers.gchart import PieChart, ColumnChart
from collections import Counter

import requests
import json
from datetime import datetime, timedelta
from log_parsing.form import UserForm, UserProfileForm
import ucd_lambda_log_supervisor as search_engine
import new_search
import os
import fnmatch

left_over_result = []

def index(request):
    context_dict = {}
    current_time = datetime.now()
    result = []
    if request.method == 'POST':
        search_query=  request.POST['search']
        start_date=  request.POST['startDate']
        end_date= request.POST['endDate']
        file_type = request.POST['fileType']
        

        # start_date = "02/01/2018 00:00"
        # end_date = "03/28/2018 00:00"
        day_list= new_search.process_date_filter(start_date, end_date)
        file_list = new_search.poke_file(day_list)

        result_list = new_search.supervisor(file_list, search_query)

        for item in result_list:
            try:
                data = json.loads(item)
                if data['1timestamp']:
                    result.append(data)
                if len(result) > 1000 :
                    break
            except:
                pass 

        request.session['result'] = result
    page = request.GET.get('page',1)
    try:
        if page >1 :
            result = request.session['result']
        paginator = Paginator(result, 100)
        paged_result = paginator.page(page)
        print len(paged_result)
        context_dict['result'] = paged_result
    except:
        pass
       
        
    return render(request, 'log_parsing/index.html', context_dict)

@login_required
def admin_page(request):
    context_dict = {}
    result ={}
    #### Load CONFIG FILES
    file = open("config.csv", "r")
    for line in file:
        if "time" not in line :
            temp = line.replace("\n","").split("=")
            result[temp[0]] = temp[1]

    event_len, log_len = chart_total_info(result['storage_path'], result['file_type'])
    log_result = get_log_resutl(result['error_report_len'])
    event_graph, log_graph = chart_daily_info(result['file_type'])

    context_dict['event_graph'] = event_graph
    context_dict['log_graph'] = log_graph
    context_dict['log_result'] = log_result
    context_dict['event_len'] = event_len
    context_dict['log_len'] = log_len
    context_dict['result'] = result
    
    return render(request, 'log_parsing/admin.html',context_dict)

def get_log_resutl(error_report_len):
    log_result = []
    file = open("log_message.log", "r")
    for line in file:
        if "Fail" in line or "does not" in line:
            log_result.append(line)
    return log_result[error_report_len:] if len(log_result)>= error_report_len else log_result

def chart_daily_info(file_type):
    file_info =[]
    file_type = file_type.split(",")

    events_list = new_search.get_date_list(file_type[0])
    logs_list = new_search.get_date_list(file_type[1])
    
    event_graph = get_chart_result(events_list)
    log_graph = get_chart_result(logs_list)

    return event_graph, log_graph

def get_chart_result(the_list):
    result = {}
    start_m= '01'
    count = 0
    for key, value in sorted(the_list.iteritems(), key=lambda (k,v): (v,k)):
        if len(key) > 0:
            month = key.split("-")[1]
            if month in result.keys():
                result[month] = int(result[month]) + int(value)
            else:
                result[month] = int(value)

    return result



def chart_total_info(storage_path, file_type):
    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    path =[]
    for item in file_type.split(","):
        path.append(dir_path+ storage_path + "/" + item)

    print path
    event_len = len(new_search._find("*.txt", path[0]))
    log_len = len(new_search._find("*.txt", path[1]))

    return event_len, log_len


def about_page(request):
    context = {}
    return render(request, 'log_parsing/about.html', context)

def user_login(request):
	context = RequestContext(request)
	if request.method == 'POST':
		username = request.POST['username']
		password = request.POST['password']
		user = authenticate(username=username, password=password)
		if user:
			if user.is_active:
				login(request, user)
				return HttpResponseRedirect('/log_parsing/')
			else:
				return HttpResponse("Your account is disable. Please contact LambdaLog team to resolve this")
		else:
			# Bad login details were provided. So we can't log the user in.
			print "Invalid login info: {0}, {1}".format(username, password)
			return HttpResponse("<h1>Invalid login credential.</h1>")
	else:
		return render_to_response('registration/login.html', {}, context)

@login_required
def user_logout(request):
	logout(request)

	# Take the user back to the homepage.
	return HttpResponseRedirect('/log_parsing/')



