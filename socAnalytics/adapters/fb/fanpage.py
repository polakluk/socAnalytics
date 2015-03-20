import facebook
import time
import re
import sqlite3
import os
import sys
from datetime import datetime

class Fanpage:
	_repeatQuery = 100 # repeat querying for 100times

	# constructor
	def __init__(self, config, db, limit):
		self.config = config # set config class for this adapter
		self.oauth_token = "" # no token so far
		self.graph = None # no graph object initialized so far
		self.limitRequest = limit # limits the number of requested data
		self.db = db # current DB connection

		self.process_id = os.getpid()
		self.job = None # currently selected job
		self.post = None # currently no post selected


	# private method that queries Facebook Graph API directly
	def _queryFacebook(self, url):
		do_query = True
		counter = 0

		while do_query:
			if len(self.oauth_token) == 0:
				self.oauth_token = facebook.get_app_access_token(self.config.fb['app_id'], self.config.fb['app_secret'])

			try:
				if self.graph == None:
					self.graph = facebook.GraphAPI(self.oauth_token)
					self.oauth_token = graph.extend_access_token(app_id=self.config.fb['app_id'], app_secret=self.config.fb['app_secret'])

				return self.graph.get_object(url)
			except:
				counter += 1
				if counter == self._repeatQuery: # try this query 100 times
					do_query = False

		return {} # return an empty dictionary


	# converts time to unix timestamp
	def _getTimestamp(self, oldTime):
		return time.mktime(time.strptime(oldTime, "%Y-%m-%dT%H:%M:%S+0000"))


	# gets list of all hashtags from text
	def _getHashTags(self, text):
		hashes = []
		res = re.findall('#[A-Za-z0-9]+', text )
		return hashes


	def _removeNonAscii(self, txt):
		return ''.join([c if ord(c) < 128 else '' for c in txt])


	# return list of tags for this object
	def _getObjectTags(self, obj, tags_key):
		if obj.has_key(tags_key) and len(obj[tags_key]) > 0:
			if isinstance(obj[tags_key], dict): # is dictionary? strange format for facebook
				values = obj[tags_key].values()
				res = []
				for val in values:
					for idx in range(0, len(val)):
						current_obj = val[idx]
						if current_obj.has_key('name') and current_obj.has_key('type'):
							res.append((current_obj['name'], current_obj['type']))

				return res
			else:		
				return [( tag['name'], str(tag['type'])) for tag in obj[tags_key]]
		else:
			return []


	# this method pages through data and automaticall calls a function to process them
	def _pageData(self, startUrl, processFunction):
		results = []
		keep_looking_inner = 1
		currentInnerUrl = startUrl

		while(keep_looking_inner):
			page_inner_object = self._queryFacebook(currentInnerUrl)

			if page_inner_object.has_key("data"):
				if len( page_inner_object["data"] ) > 0:
					for data in page_inner_object['data']:
						results.append( processFunction(data))

					if page_inner_object['paging'].has_key( 'next' ): # go to the next page
						currentInnerUrl = page_inner_object['paging']['next'][32:]
					else: # end of data so you're done
						keep_looking_inner = 0
				else:
					keep_looking_inner = 0
					break
			else:
				keep_looking_inner = 0
				break

		return results


	# this method created a dictionary representation of a single comment with its replies
	def _processFunctionCommentItself(self, comment):
		return {
			"created" : self._getTimestamp(comment['created_time']),
			"message" : self._removeNonAscii(comment['message']),
			"id" : str(comment["id"]),
			"from" : comment["from"]["id"],
			"name" : self._removeNonAscii(comment["from"]["name"]),
			"likes" : comment["like_count"],
			"hashes" : self._getHashTags(comment['message']),
			"tags" : self._getObjectTags(comment, "message_tags"),
			"comments" : []
			}


	# process function for paging through comments 
	def _processFunctionComment(self, data):
		res = self._processFunctionCommentItself(data)
		currentInnerUrl = res["id"] + "/comments?limit="+str(self.limitRequest)

		res["comments"] = self._pageData(currentInnerUrl, self._processFunctionCommentItself) # get all replies to this comment
		return res


	# process function for paging through likes
	def _processFunctionLikes(self, data):
		return data["name"]


	# inserts/updates page information in db
	def _updatePageInformation(self, page):
		curs = self.db.GetCursor()

		curs.execute( "SELECT page_id FROM Pages WHERE url = ?", (self.job[1], ) )

		origRow = curs.fetchone()
		pageId = 0

		# update basic info about the page
		if origRow == None: # this page does not exist in our DB
			data = (page['name'], self.job[1], page['category'])
			curs.execute("INSERT INTO `Pages` (`name`, `url`, `category`, `last_modified`) VALUES(?,?,CURRENT_TIMESTAMP,?) ", data)
			pageId = curs.lastrowid
		else: # this page does exist in our DB
			data = (page['name'], self.job[1], page['category'], origRow[0])
			pageId = origRow[0]
			curs.execute("UPDATE `Pages` SET `name` = ?, `url` = ?, `category` = ?, `last_modified` = CURRENT_TIMESTAMP WHERE page_id = ?", data)

		# store current statistics about the page
		talkingAbout = 0
		if page.has_key("talking_about_count"):
			talkingAbout = page['talking_about_count']
		curs.execute("INSERT INTO `PagesInfo` (`page_id`, `likes`, `talking_about`, `created`) VALUES (?,?, ?, CURRENT_TIMESTAMP)", (pageId, page['likes'], talkingAbout) )
		self.db.Commit()


	# find job for this process
	def _findJob(self):
		keep_looking = True

		while keep_looking:
			curs = self.db.GetCursor()
			curs.execute("SELECT `job_id`, `job_content_id`, `date_from`, `date_to` FROM `Jobs` WHERE `crawler_id` = 0 AND `job_type` = 'fbFanpage' LIMIT 1" )
			self.job = curs.fetchone()

			if self.job == None: # no job is left for me to work on
				keep_looking = False
				break

			if self.config.debug == False:
				curs.execute("UPDATE Jobs SET crawler_id = ? WHERE crawler_id = 0 AND job_type = 'fbFanpage' AND `job_id` = ?", (self.process_id, self.job[0]))
			else:
				self.db.Commit()
				return True

			self.db.Commit()
			if curs.rowcount == 1: # found my job
				return True

		return False


	# find post to be crawled for this process
	def _findPostCrawl(self):
		keep_looking = True

		while keep_looking:
			curs = self.db.GetCursor()
			curs.execute("SELECT `post_id`, `post_fb_id` FROM `PostsCrawler` WHERE `crawler_id` = 0 LIMIT 1" )
			self.post = curs.fetchone()

			if self.post == None: # no post is left for me to work on
				keep_looking = False
				break

			curs.execute("UPDATE PostsCrawler SET crawler_id = ? WHERE crawler_id = 0 AND `post_id` = ?", (self.process_id, self.post[0]))

			self.db.Commit()
			if curs.rowcount == 1: # found my post
				return True

		return False


	# this mathod stores list of posts that should be further processed in separate scripts
	def ProducePosts(self):
		if self._findJob(): # try to find job for yourself
			page = self._queryFacebook(self.job[1])
			if page == None: # no page was found or couldnt be fetched
				return
			self._updatePageInformation(page) # update page information

			# start producing posts for crawling
			keep_looking = True
			currentUrl = self.job[1] + "/posts?fields=id&limit="+str(self.limitRequest)
			while keep_looking:
				data = self._queryFacebook(currentUrl)

				# store all relevant posts to DB
				if len(data['data']) > 0:
					cur = self.db.GetCursor()
					for row in data['data']: # walk through all posts
						post_time = self._getTimestamp(row["created_time"])
						if(post_time > self.job[2]) :
							print("Loaded post: " + row["id"] + " (" + row["created_time"] + ")" )
							cur.execute( "INSERT INTO `PostsCrawler` (`crawler_id`, `post_fb_id`, `created`) VALUES( 0, ? , CURRENT_TIMESTAMP) ", (str(row["id"]),) )
						else: # we loaded the last post so stop here
							keep_looking = False
							break

					self.db.Commit()

				currentUrl = data['paging']['next'][32:] # go to the next page as long as you can


	# stores fetched post from FB to DB and return its DB ID
	def _storePostIntoDb(self, post):
		postId = None
		msg = ""
		tags = ""
		objId = ""
		shares = 0
		if post.has_key( 'message' ):
			msg = post['message']
			tags = self._getObjectTags(post, "message_tags")
		else:
			if post.has_key( 'story' ):
				msg = post['story']
				tags = self._getObjectTags(post, "story_tags")
			else:
				return postId

		if post.has_key("object_id"):
			objId = post["object_id"]
		else:
			if post.has_key('id'):
				objId = post['id']		
		if post.has_key('shares'):
			shares =  post['shares']['count']

		created_fb = self._getTimestamp(post["created_time"])
		data = (str(objId), str(self._removeNonAscii(msg)), shares, str(post["type"]), ",".join([ self._removeNonAscii(tag[0])+"|"+tag[1] for tag in tags ]), created_fb)
		curr = self.db.GetCursor()
		curr.execute("INSERT INTO `Posts` (`post_fb_id`, `msg`, `likes`, `comments`, `shares`, `type`, `tags`, `created`, `created_fb`) VALUES( ?, ?, 0, 0, ?, ?, ?, CURRENT_TIMESTAMP, ?)", data)
		postId = curr.lastrowid

		self.db.Commit()

		return postId


	# this method stores a comment into DB and keeps parent-child relationshop for when it is necessarry
	def _storeCommentInDb(self, post_id, comment, parent_id):
		commentId = None
		tags = ""
		hashes = ""

		if len(comment['hashes']) > 0:
			hashes = "|".join(comment['hashes'])

		if len(comment['tags']) > 0:
			tags = "|".join([ self._removeNonAscii(tag[0])+"|"+tag[1] for tag in comment['tags'] ])

		data = (comment['id'], parent_id, post_id, comment['name'], comment['from'], str(comment['likes']), comment['message'], tags, hashes)

		curr = self.db.GetCursor()
		curr.execute("INSERT INTO `Comments` (`comment_fb_id`, `parent_id`, `post_id`, `author_name`, `author_id`, `likes`, `msg`, `tags`, `hashes`, `created`) VALUES( ?,?,?,?,?,?,?,?,?, CURRENT_TIMESTAMP)", data)
		commentId = curr.lastrowid

		self.db.Commit()

		if len(comment['comments']) > 0:
			for comm_nested in comment["comments"]:
				self._storeCommentInDb(post_id, comm_nested, commentId)


	# This method reads posts waiting to be crawled from DB and process them
	def ProducePost(self):
		while self._findPostCrawl(): # try to find next post to be crawled
			post = self._queryFacebook(self.post[1] + "?limit=5")
			post_id = self._storePostIntoDb(post)

			if post_id == None: # could not fetch the post so skip it
				continue

			# fetch all comments
			print("Reading comments: "+self.post[1])
			comments = self._pageData(self.post[1] + "/comments?limit=" + str(self.limitRequest), self._processFunctionComment)
			print("Finished reading comments: "+self.post[1])
			
#			# store them in DB
			if comments != None and len(comments) > 0:
				for comment in comments:
					self._storeCommentInDb(post_id, comment, 0)
			print("Finished storing: "+self.post[1])



	# adds job to DB
	# format: CWArrow (ID) 20/12/2014 (until date)
	def AddJob(self ):
		curs = self.db.GetCursor()

		dataSql = ("fbFanpage", sys.argv[2], int(time.mktime(datetime.strptime(sys.argv[3],'%d/%m/%Y').timetuple())))
		curs.execute("INSERT INTO `Jobs` (`job_type`, `job_content_id`, `date_from`, `date_to`, `crawler_id`) VALUES(?,?, ?,CURRENT_TIMESTAMP,0) ", dataSql)
		self.db.Commit()
