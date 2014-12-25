#!/usr/local/bin/python3.3 
# -*- coding: utf8 -*-

import sys, traceback, codecs
from urllib.parse import urlparse
from imp import reload
from time import strftime, gmtime, time
from datetime import date, datetime, timedelta
import boto.sns, json, sys, csv
from elasticsearch import Elasticsearch
from boto.sns import SNSConnection

##################################################################
# 
# By using the URI interface to push message to AWS service.
# Message format:
#
#  { "GCM":"{\"data\":{\"default\":\"<content>\", \"title\":\"<title>\"}}" }
#
# export AWS_ACCESS_KEY_ID=
# export AWS_SECRET_ACCESS_KEY=
#
##################################################################
def connect_sns(region, aws_keyid, aws_accesskey, debug):
	conn = boto.sns.connect_to_region(region, 
		aws_access_key_id=aws_keyid, 
		aws_secret_access_key=aws_accesskey, debug=0)
	return conn

##################################################################
#
# Add an endpoint before publishing
#
##################################################################
def add_endpoint(conn, app_arn, token, uid):
	try:
		endpoint = conn.create_platform_endpoint(platform_application_arn=app_arn, token=token, custom_user_data=uid)
		#print(endpoint)
		return endpoint
	except:
		print("Failed to add endpoint for uid: " + uid)

##################################################################
# 
# Publish a single message to an endpoint.
#
##################################################################
def publish(conn, subject, endpoint, msg, rolename, token, var):
	if var != None:
		#msg = msg.format( rolename, tuple(var) )
		msg = msg.format( rolename )
	with open('gcmresult.log', 'a', encoding='utf-8') as logfile:
		timestr = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
		try:
			result =  conn.publish(message=msg,
			target_arn=endpoint['CreatePlatformEndpointResponse']['CreatePlatformEndpointResult']['EndpointArn']
			, message_structure='json')
			#print('===========================================')
			#print(timestr)
			response = result.get('PublishResponse')
			if response:
				publishResult = response.get('PublishResult')
				if publishResult:
					messageId = publishResult.get('MessageId')
					print('success, {}, {}, {}, {}'.format(timestr, rolename, token, messageId), file=logfile)
				else:
					print('fail, {}, {}, {}, {}'.format(timestr, rolename, token, messageId), file=logfile)
			else:
				print('fail, {}, {}, {}, {}'.format(timestr, rolename, token, messageId), file=logfile)
		except Exception as err:
			print('failed to send to endpoint:{}', endpoint)
			print('fail, {}, {}, {}, {}'.format(timestr, rolename, token, messageId), file=logfile)
			traceback.print_exc(file=sys.stdout)

##################################################################
# 
# Read user list from the data file. The file has the following format
#
#  rolename,token
#
# The fields after token will be used to format the message content
#
##################################################################
def read_user(userfile):
	users = dict()
	for row in csv.reader(open(userfile, 'rU', encoding="utf-8"), delimiter=','):
		if len(row) < 2:
			continue
		if row[0].startswith('#'):
			continue
		rolename = row[0]
		token = row[1]
		variables=list()
		if len(row)>2:
			variables=row[2:]
		userdict = {}
		userdict['rolename'] = rolename
		userdict['var'] = variables
		users[token] = userdict
	print(users)
	return users

##################################################################
# 
# Read user list from the database
#  {
#    "_index": "logstash-2014.12.24",
#    "_type": "logs",
#    "_id": "k04UneW6RyWZ5kWPn300uQ",
#    "_score": 1,
#    "_source": {
#    "message": "新兵.影狼`SC-06D`APA91bF822itUwIa0IQYf9B7izSYtkKFTadaBlNgxtY8rSlsTrUW48t7uUo5sIatzhLppZlhVmKx4d_nXidAgM5I2qYvElWFSoWTpoCVVD_CdclH8_GyNOGRvmeiAEA0VyLuODlj1bjA0EeLjxwnqk1GtXseou6WT8YNmhrnw2c0N0Nil7QIS6xzeYArlxZXfqM447eu_Kxq`97.43.168.192",
#    "@version": "1",
#    "@timestamp": "2014-12-24T12:00:13.961Z",
#    "host": "10.0.182.79:18501",
#    "rolename": "新兵.影狼",
#    "device": "SC-06D",
#    "devicetoken": "APA91bF822itUwIa0IQYf9B7izSYtkKFTadaBlNgxtY8rSlsTrUW48t7uUo5sIatzhLppZlhVmKx4d_nXidAgM5I2qYvElWFSoWTpoCVVD_CdclH8_GyNOGRvmeiAEA0VyLuODlj1bjA0EeLjxwnqk1GtXseou6WT8YNmhrnw2c0N0Nil7QIS6xzeYArlxZXfqM447eu_Kxq",
#    "ip": "97.43.168.192"
#    }
#  },
#
##################################################################
def read_user_elasticsearch(url):
	yesterday = (date.today() - timedelta(days=1)).strftime('%Y.%m.%d')
	try:
		esurl = urlparse(url)
		#print(esurl.hostname + ":" + str(esurl.port))
		es = Elasticsearch(esurl.hostname + ":" + str(esurl.port))
		index = 'logstash-' + yesterday
		body = {
	        "filter": {
	            "not": {
	                "term": {
	                    "tags": "_grokparsefailure"
	                }
	            }
	        },
	        "query": {
	            "match_all": {}
	        }
		}
		res = es.search(index=index, body=body)
		users = dict()
		for hit in res['hits']['hits']:
			#print(hit)
			token = hit.get('_source').get('devicetoken')
			rolename = hit.get('_source').get('rolename')
			if token and rolename:
				variables=[rolename]
				userdict = {}
				userdict['rolename'] = rolename
				userdict['var'] = {}
				users[token] = userdict
		#print(users)
		return users
	except Exception as err:
		print("Elasticsearch query error: %s"%err)
		traceback.print_exc(file=sys.stdout)

###########################################################
# Read the aws config file and push config file to prepare 
# pushing.
#   select nickname, uid, login_time, init_time, pushtoken
#   from user u, push_token p
#   where u.gid = p.uid
#   and p.uid = '316779620332537'
###########################################################
if __name__ == "__main__":
	reload(sys)
	#sys.setdefaultencoding('utf8')
	if len(sys.argv) < 4:
		print('Usage: <configfile> <messagefile> <userfile>|<elasticsearch_url>')
		sys.exit(-1)
	configfile = sys.argv[1]
	messagefile = sys.argv[2]
	userfile = sys.argv[3]
	readFromFile = True
	if userfile.startswith('http://'):
		readFromFile = False
	with open(configfile, 'r') as awsfile:
		awsconfig = json.load(awsfile)
	subject = awsconfig.get('aws_subject')
	aws_topic_arn = awsconfig.get('aws_topic_arn')
	aws_keyid = awsconfig.get('aws_keyid')
	aws_region = awsconfig.get('aws_region')
	aws_accesskey = awsconfig.get('aws_accesskey')
	debug = awsconfig.get('debug')
	print('-- aws config ---')
	print('aws_topic_arn:\t%s '%aws_topic_arn)
	print('aws_region:\t%s '%aws_region)
	print('aws_keyid:\t%s '%aws_keyid)
	print('aws_accesskey:\t%s '%aws_accesskey)
	print('debug:\t%s '%debug)
	
	boto.set_stream_logger('push')

	with codecs.open(messagefile, 'r', 'utf-8') as msgfile:
		msgconfig = json.load(msgfile)
	title = msgconfig.get('title')
	message = msgconfig.get('message')
	print('title:\t%s'%title)
	print('message:\t%s'%message)
	
	message = u'{{"GCM":"{{\\"data\\":{{\\"default\\":\\"%s\\", \\"title\\":\\"%s\\"}}}}"}}' % (message,title)
	if readFromFile:
		users = read_user(userfile)
	else:
		print(userfile)
		users = read_user_elasticsearch(userfile)
	conn = connect_sns(aws_region, aws_keyid, aws_accesskey, debug)
	line = 0
	for token in users.keys():
		userdict = users.get(token)
		rolename = userdict.get('rolename')
		var = userdict.get('var')
		print('token={0},rolename={1}'.format(token,rolename))
		endpoint = add_endpoint(conn, aws_topic_arn, token, rolename)
		publish(conn, 'Subject', endpoint, message, rolename, token, var)
		line+=1
		print("line:%s, rolename:%s"%(line,rolename))



