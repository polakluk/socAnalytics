import facebook
import time
import re
import sqlite3

class Fanpage:
	_repeatQuery = 100 # repeat querying for 100times

	# constructor
	def __init__(self, config, db, limit):
		self.config = config # set config class for this adapter
		self.oauth_token = "" # no token so far
		self.graph = None # no graph object initialized so far
		self.limitRequest = limit # limits the number of requested data
		self.db = db # current DB connection


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


	# return list of tags for this object
	def _getObjectTags(self, obj, tags_key):
		if obj.has_key(tags_key) and len(obj[tags_key]) > 0:
			if isinstance(obj[tags_key], dict): # is dictionary? strange format for facebook
				values = obj[tags_key].values()
				res = []
				for val in values:
					for idx in range(0, len(val)):
						current_obj = val[idx]
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
			"message" : comment['message'],
			"id" : str(comment["id"]),
			"from" : comment["from"]["id"],
			"name" : comment["from"]["name"],
			"likes" : comment["like_count"],
			"hashes" : self._getHashTags(comment['message']),
			"tags" : self._getObjectTags(comment, "message_tags"),
			"comments" : []
			}


	# process function for paging through comments 
	def _processFunctionComment(self, data):
		res = self._processFunctionCommentItself(data)
		currentInnerUrl = res["id"] + "/comments?limit="+self.limitRequest

		res["comments"] = self._pageData(currentInnerUrl, self._processFunctionCommentItself) # get all replies to this comment
		return res


	# process function for paging through likes
	def _processFunctionLikes(self, data):
		return data["name"]

	# reads and stores/updates information about the fan page
	def _readPageData(pageName):
		conn = sqlite3.connect("db.lite")


	# inserts/updates page information in db
	def _updatePageInformation(self, pageName):
		conn = sqlite3.connect("db.lite")
		curs = conn.cursor()

		curs.execute( "SELECT page_id FROM pagea WHERE url = '?'", (pageName) )

		origRow = curs.fetchone()
		data = [()]
		if origRow == None: # this page does not exist in our DB
			print()
		else: # this page does exist in our DB
			print()
		conn.close()


	# this mathod stores list of posts that should be further processed in separate scripts
	def producePosts(self, pageName, dataTo):
		self._updatePageInformation(pageName)
