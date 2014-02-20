#!/usr/bin/env python
import pika
import tweepy
import sys
import json
import time
import math
from collections import defaultdict
import random

#set up cities dict
cities = {}
cities["Greater_London"] = 1
cities["Greater_Manchester"] = 0.260883607
cities["West_Midlands"] = 0.2494002
cities["West_Yorkshire"] = 0.181654911
cities["Liverpool"] = 0.088288994
cities["South_Hampshire"] = 0.087415118
cities["Tyneside"] = 0.079172093
cities["Nottingham"] = 0.074583144
cities["Sheffield"] = 0.070025357
cities["Bristol"] = 0.063068676
cities["Leicester"] = 0.051996919
cities["Brighton_and_Hove"] = 0.048479038
cities["Bournemouth_Poole"] = 0.047639287
cities["Teesside"] = 0.038481313
cities["Stoke_on_Trent"] = 0.038087133
cities["Coventry"] = 0.036706484
cities["Sunderland_Wearside"] = 0.034269991
cities["Birkenhead"] = 0.033232844
cities["Reading"] = 0.032492098
cities["Kingston_upon_Hull"] = 0.032083819
cities["Preston_Central_Lancashire"] = 0.032012707
cities["Southend_on_Sea"] = 0.030172386
cities["Derby"] = 0.027634232
cities["Plymouth"] = 0.026585437
cities["Luton"] = 0.026362192
cities["Farnborough_Aldershot"] = 0.025787883
cities["Medway_Towns"] = 0.024922896
cities["Blackpool"] = 0.024460875
cities["Milton_Keynes"] = 0.023493511
cities["Barnsley_Dearne_Valley"] = 0.022813046
cities["Northampton"] = 0.022065352
cities["Norwich"] = 0.021779577
cities["Swindon"] = 0.018964026
cities["Crawley"] = 0.018442847
cities["Ipswich"] = 0.018271913
cities["Wigan"] = 0.017921464
cities["Mansfield"] = 0.017569277
cities["Oxford"] = 0.017510222
cities["Warrington"] = 0.016904955
cities["Slough"] = 0.016733409
cities["Peterborough"] = 0.016692744
cities["Cambridge"] = 0.016187504
cities["Doncaster"] = 0.016157568
cities["York"] = 0.015705559
cities["Gloucester"] = 0.015331201
cities["Burnley"] = 0.015266731
cities["Telford"] = 0.015119399
cities["Blackburn"] = 0.01497033
cities["Basildon"] = 0.014800521
cities["Grimsby"] = 0.013707383
cities["Hastings"] = 0.01363198
cities["High_Wycombe"] = 0.013609707
cities["Thanet"] = 0.012809292
cities["Accrington_Rossendale"] = 0.012777517
cities["Burton_upon_Trent"] = 0.012485305
cities["Colchester"] = 0.012450567
cities["Eastbourne"] = 0.012078661
cities["Exeter"] = 0.012032071
cities["Cheltenham"] = 0.011897612
cities["Paignton_Torquay"] = 0.01179166
cities["Lincoln"] = 0.011737407
cities["Chesterfield"] = 0.01155125
cities["Chelmsford"] = 0.011393292
cities["Basingstoke"] = 0.010997989
cities["Maidstone"] = 0.010996456
cities["Bedford"] = 0.010926264
cities["Worcester"] = 0.010386694
cities["Cardiff"] = 0.045700167
cities["Newport"] = 0.031350837
cities["Swansea"] = 0.030687537
cities["Greater_Glasgow"] = 0.119364376
cities["Edinburgh"] = 0.046201524
cities["Aberdeen"] = 0.020161378
cities["Dundee"] = 0.015803338
cities["Falkirk"] = 0.009338717
cities["East_Kilbride"] = 0.007539878
cities["Greenock"] = 0.007519342
cities["Blantyre_Hamilton"] = 0.006730472
cities["Ayr_Prestwick"] = 0.006269779
cities["Livingston"] = 0.006080352
cities["Belfast_Metropolitan"] = 0.059214139
cities["Derry"] = 0.00927067
cities["Craigavon"] = 0.005893787

allusers = defaultdict(list)

debug = False

#divide users into cities: "city -> [user]"
with open(sys.argv[1], 'r') as f:
	for user in f:
		userid,location = user.strip().split(',')
		allusers[location].append(userid)

#pick a proportionate number of users (these users are already randomly picked, so no need to pick a random subset)
longest = len(allusers['Greater_London'])
for city, users in allusers.iteritems():
	#we want this many users for this city
	amount = long(math.ceil(longest * cities[city]))
	
	if debug:
		print city, amount

	# shorten the list of users. no need to pick a random set, as the users have already been shuffled
	# by the previous script that generated usersample.csv
	allusers[city] = allusers[city][:amount]

#print the users
#for city, userids in allusers.iteritems():
#	print city + "\t" + str(float(len(userids)) / float(len(allusers['Greater_London'])))
userids = []
for city, users in allusers.iteritems():
	print city
	for user in users:
		userids.append(user)
		if debug:
			print "\t" + user

# for good measure, make sure users aren't sorted by city
random.shuffle(userids)
for user in userids:
	print user