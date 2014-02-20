#!/usr/bin/env python
import pika
import tweepy
import sys
import json
import time
import math
from collections import defaultdict
import random
from twython import Twython, TwythonError, TwythonRateLimitError
from shove import Shove
from dateutil import parser
import datetime

class Timelineparser:
	
	def run(self):
		self.sinceIds = Shove('file://since_id_store_' + sys.argv[1])
		self.tweetsPerSec = Shove('file://tweets_per_day_store_' + sys.argv[1])
		self.lastParsed = Shove('file://last_parsed_store_'  + sys.argv[1])

		APP_KEY = sys.argv[2]
		APP_SECRET = sys.argv[3]
		
		ACCESS_TOKEN = sys.argv[4]
		ACCESS_TOKEN_SECRET = sys.argv[5]
		
		self.twitter = Twython(APP_KEY, APP_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

		filelen = 0
		with open(sys.argv[1], 'r') as f:						
			for _ in f:
				filelen += 1

		with open(sys.argv[1], 'r') as f:				
			i = 0
			for userid in f:
				i += 1
				while True:	

					log("User " + str(i) + " of " + str(filelen) + " (" + str((float(i)/float(filelen))*100.0) + "%)")

					userid = long(str(userid).strip())
					try:
						self.getTweets(userid)
						self.rateLimitCheck()
						break
					except TwythonRateLimitError:
						self.rateLimitCheck()
					except TwythonError,e:
						if "401" in str(e):
							print "401 UNAUTHORIZED FOR USER " + str(userid) + ", SKIPPING"
							break
						elif "Max retries exceeded with url" in str(e):
							print "MAX RETRIES EXCEEDED, RETRYING IN 30 SECS"
							time.sleep(30)
						else:
							raise
					except Exception,e:
						if "Connection reset by peer" in str(e):
							print "CONNECTION RESET BY PEER RETRING IN 60 SECS"
							time.sleep(60)
						else:
							raise

	def getTweets(self, userid):
		
		# Get the last tweet id we have for this user
		try:
			lastId = self.sinceIds[str(userid)]
		except:
			lastId = None

		if (lastId == None):
			# If the lastId is None, then we want to parse their timeline from newest to oldest (with a limit on the max amount of tweets)
			log("NEW USER " + str(userid) +  " FETCHING TIMELINE FROM NEWEST TO OLDEST")
			self.newestToOldest(userid)
		else:
			# If we DO have a lastId, then we want the tweets SINCE this date.						
			while True:
				try:
					tweetsPerSec = self.tweetsPerSec[str(userid)]
					secsSinceLast = (datetime.datetime.now() - self.getLastParsed(userid)).total_seconds()
					break
				except:
					log("COULD NOT FIND LASTPARSED FOR USER " + str(userid) + ", DOING SAFE FETCH-ALL")
					self.newestToOldest(userid, lastId)
			
			# force tweetsPerSec to not get too big
			if tweetsPerSec > 0.0333:
				tweetsPerSec = 0.0333
			expectedNewTweets = tweetsPerSec * secsSinceLast

			if (expectedNewTweets < 10):
				log("ONLY EXPECTING " + str(expectedNewTweets) + " NEW TWEETS FOR USER " + str(userid) + ", SKIPPING...")
			else:
				log("EXPECTING " + str(expectedNewTweets) + " FOR USER " + str(userid))
				self.since(userid, lastId)

	def since(self, userid, lastId):
		
		# always contains the id we want to fetch tweets SINCE (we'll get newer tweets)
		# this changes every iteration of the while loop, as we get new pages
		# will be None in first iteration
		sinceid = lastId

		# the id of the newest tweet we've seen of all for this user
		newestId = lastId

		# amount of tweets parsed
		count = 0

		# tweets per seconds for this user.
		tweetsPerSec = -1.0

		# list of tweets fetched
		tweets = []

		while True:
			log("\tFETCHING TWEETS FOR USER " + str(userid) + " SINCE " + str(sinceid))
			
			# get <=200 tweets NEWER than sinceid
			timeline = self.twitter.get_user_timeline(user_id=userid, since_id=sinceid, count=200)

			# if no more tweets, then persist all info and stop
			if len(timeline) == 0:
				log("\tDONE, NO NEWER TWEETS THAN " + str(lastId))
				self.persist(userid, tweets, newestId, tweetsPerSec)
				return			

			# the first tweet of every timeline we get will always be the NEWEST tweet we know of.
			newestId = timeline[0]['id']
			
			# get an estimate of tweets per sec, based on the newest list of tweets we've got
			if (len(timeline) >= 2):
				tweetsPerSec = self.calcTweetsPerSec(userid, timeline)

			log("\tGOT " + str(len(timeline)) + " TWEETS")
			for tweet in timeline:
				count += 1
				tweets.append(tweet)

			# do a guess as to whether we'll get more unknown tweets if we do another query
			if len(timeline) < 150:
				# we got fewer tweets than requested (200), so it seems unlikely that there would be
				# any newer tweets. Don't waste another request on getting an empty list of tweets!
				# remember to save updated tweets-per-sec before stopping
				self.persist(userid, tweets, newestId, tweetsPerSec)
				log("\tDONE, ONLY FETCHED " + str(len(timeline)) + ", SO ASSUMING NO NEWER TWEETS. LAST ID: " + str(lastId))
				return
			else:
				# there's like more new tweets than what's on this page
				sinceid = timeline[0]['id']

	def persist(self, userid, tweets, newestId, tweetsPerSec):
		
		if (tweetsPerSec > 0.0):
				self.putTweetsPerSec(userid, tweetsPerSec)
				self.putLastParsed(userid)
				log("\tPERSISTED TWEETS-PER-SEC " + str(tweetsPerSec) + " FOR USER " + str(userid))

		if len(tweets) > 0:
			self.putTweets(userid, tweets)

			self.sinceIds[str(userid)] = newestId
			self.sinceIds.sync()

			log("\tPERSISTED " + str(len(tweets)) + " TWEETS FOR USER " + str(userid))
		else:
			log("\tNO TWEETS, NOT PERSISTING TWEETS FOR USER " + str(userid))

	def newestToOldest(self, userid, stopAtId=None):
		
		# the at any time oldest tweet we know of
		oldestId = None

		# is first pass pass through the while loop?
		first = True

		# limit how many pages of tweets we actually want to fetch per user 
		maxPages = 5

		# tweets per seconds for this user.
		tweetsPerSec = -1.0

		# tweets found
		tweets = []

		# id of newest tweet we know of
		newestId = None

		while True:				
			log("\tFETCHING TWEETS FOR USER " + str(userid) + " OLDER THAN " + str(oldestId))
			
			# See "Optimizing max_id for environments with 64-bit integers"
			# on https://dev.twitter.com/docs/working-with-timelines
			if oldestId == None:
				maxid = None
			else:
				maxid = oldestId-1

			# get <=200 tweets OLDER than maxid
			timeline = self.twitter.get_user_timeline(user_id=userid, max_id=maxid, count=200)
			maxPages -= 1
			
			#No more tweets on timeline
			if len(timeline) == 0:
				log("\tDONE, RECEIVED EMPTY TIMELINE")
				self.persist(userid, tweets, newestId, tweetsPerSec)
				return

			#We got tweets!
			#remember the first tweet, so we can do a SINCE tweets next time
			if first:
				newestId = self.sinceIds[str(userid)] = timeline[0]['id']
				first = False

				#estimate tweets per day based on the newest page of tweets
				tweetsPerSec = self.calcTweetsPerSec(userid, timeline)

			log("\tGOT " + str(len(timeline)) + " TWEETS")
			for tweet in timeline:
				if stopAtId != None and tweet['id'] == stopAtId:					
					log("\tDONE, REACHED STOP_AT_TWEET_ID " + str(stopAtId))
					self.persist(userid, tweets, newestId, tweetsPerSec)
					return
				
				tweets.append(tweet)

			oldestId = timeline[len(timeline)-1]['id']

			#don't fetch more than n pages!
			if maxPages == 0 or len(timeline) < 150:								
				if len(timeline) < 150:
					log("\tDONE, TIMELINE HAS " + str(len(timeline)) + " TWEETS, SO ASSUMING IT'S LAST PAGE")
				else:
					log("\tDONE, PARSED MAX NO OF PAGES")
				self.persist(userid, tweets, newestId, tweetsPerSec)
				return

	def close(self):
		self.sinceIds.close()
		self.tweetsPerSec.close()
		self.lastParsed.close()

	def putLastParsed(self, userid):		
		self.lastParsed[str(userid)] = datetime.datetime.now().isoformat()
		self.lastParsed.sync()

	def getLastParsed(self, userid):
		return parser.parse(self.lastParsed[str(userid)])

	def calcTweetsPerSec(self, userid, timeline):
		
		firstTweet = parser.parse(timeline[0]['created_at'])
		lastTweet = parser.parse(timeline[len(timeline)-1]['created_at'])

		tdelta = firstTweet - lastTweet
		tdeltaSecs = tdelta.total_seconds()
		
		#avoid division by zero errors
		if tdeltaSecs < 0.01:
			tdeltaSecs = 0.01

		tweetsPerSec = len(timeline) / tdeltaSecs		
		
		return tweetsPerSec

	def putTweetsPerSec(self, userid, tweetsPerSec):
		self.tweetsPerSec[str(userid)] = tweetsPerSec
		self.tweetsPerSec.sync()

	def putTweets(self, userid, tweets):
		
		today = datetime.date.today().strftime("%Y%m%d")

		with open('tweets_' + sys.argv[1] + "_" + today + '.list', 'a') as tweetsfile:
			for tweet in tweets:
				tweetsfile.write(json.dumps(tweet) + '\n')
			
	def rateLimitCheck(self):		
		try:
			remaining = self.twitter.get_lastfunction_header('x-rate-limit-remaining')
			resetTime = self.twitter.get_lastfunction_header('x-rate-limit-reset')		

			log(str(remaining) + " REQUESTS LEFT. RESETS AT " + str(resetTime))

			if (remaining == "0"):
				# sleep until secsUntilReset
				now = int(time.time())
				diff = int(resetTime) - now
				log("RATE LIMIT HIT, SLEEPING FOR " + str(diff + 3) + " SECONDS")
				time.sleep(diff + 3)
				pass				
				
		except TwythonError:
			pass	

runNo = 0

def log(output):
	global runNo
	print str(runNo) + "," + time.strftime("%Y%m%d %H:%M:%S") + ": " + output
	sys.stdout.flush()

def run():
	global runNo
	while True:
		runNo += 1
		closed = False
		timelineparser = Timelineparser()	
		try:
			timelineparser.run()
			timelineparser.close()
			return
		except TwythonError,e:
			log("Caught Unknown TwythonError, closing files and retrying in 30 seconds")
			log(str(e))
			timelineparser.close()
			time.sleep(30)
			log("Retrying now!")
		except Exception,e:	
			log("Caught Unknown Exception, closing files!")
			log(str(e))
			timelineparser.close()
			closed = True
			raise
run()