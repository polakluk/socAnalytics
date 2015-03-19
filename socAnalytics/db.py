import sqlite3

# class that manages DB connection
class Db:

	# opens a connection to a file with SQLite3 DB
	def __init__(self, f):
		self.db_file = f
		self.conn = sqlite3.connect(self.db_file)


	# commits data to the DB file
	def Commit(self):
		self.conn.commit()


	# returns cursor for this connection
	def GetCursor(self):
		return self.conn.cursor()


	# closes connection to the DB
	def Close(self):
		self.conn.close()