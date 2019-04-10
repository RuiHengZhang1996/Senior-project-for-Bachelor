from django.conf.urls import patterns, url
from log_parsing import views
from django.contrib import admin
from django.contrib.auth import views as auth_views

urlpatterns = patterns('',
        url(r'^$', views.index, name='index'),
		url(r'^login/$', views.user_login, name='login'),
		url(r'^logout/$', views.user_logout, name='logout'),
		url(r'^admin/$', views.admin_page, name='admin_page'),
		url(r'^about/$', views.about_page, name='about_page'),
        )