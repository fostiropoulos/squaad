import psycopg2
import datetime
import json
import sys
from squaad import file
import os
"""
Utilities and functions for retreiving specific type of data from the squaad Database.
Makes working with the data easier and avoids re-writing queries. Also makes sure the queries
used for the analysis are correct since they are validated.
"""
class db():
	cur=None;

	cache_folder=None
	"""
		For the config file and format please look the repository for an example
		{"pgsql":{"host":"","user":"","passwd":"","db":""} }
		Support only for postgresql. Database must have the tables and fields of
		SQUAAD Database

		@config_file path to config file that contains the connection information
		@cache_folder path to the cache folder where to save the results
	"""
	def __init__(self, config_file , cache_folder=None):
		if(cache_folder!=None):
			if(not os.path.isdir(cache_folder)):
				raise Exception("Cache folder %s doesn't exist"%cache_folder)

		try:
			with open(config_file) as json_data_file:
				config = json.load(json_data_file)
		except Exception as e:
			print("Could not laod configuration file: "+config_file)
			print(e)
			raise
		#print("Connecting to Database: ")
		try:
			conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'"%(config["pgsql"]["db"],config["pgsql"]["user"],config["pgsql"]["host"],config["pgsql"]["passwd"],))
		except Exception as e:
			print ("I am unable to connect to the database")
			print(e)
			raise
		self.cur = conn.cursor()
		self.testConnection()
		self.cache_folder=cache_folder
		#print("Connected")
	"""
	Tests connection and returns True if it can connect otherwise raises exception.
	"""
	def testConnection(self):
		try:
			self.cur.execute("SELECT 1")
			return True
		except Exception as e:
			print(e)
			raise
	"""
		Helper function, reads sql file into a string
		@name name of SQL file to read. Stored internally in sql folder
	"""
	def getQuery(name):
		with open(os.path.join(os.path.dirname(__file__), 'sql',name), 'r') as queryFile:
			query = queryFile.read()
		return query

	"""
		If cache folder exists, load object from it
		@name name of cache object
	"""
	def readCache(self,name):
		if(self.cache_folder!=None):
			return file.file.load_obj(os.path.join(self.cache_folder,name))
		return None
	"""
		If cache folder exists, save object to it
		@data to save
		@name name of cache object
	"""
	def saveCache(self,data,name):
		if(self.cache_folder!=None):
			file.file.save_obj(data,os.path.join(self.cache_folder,name))

	"""
		Executes query and checks if it is in cache
	"""
	def executeQueryWithCache(self,queryFile):

		results=self.readCache(queryFile)
		if(results!=None):
			return results

		query=db.getQuery(queryFile)
		self.cur.execute(query)

		results = self.cur.fetchall()
		columns = [column[0] for column in self.cur.description]
		formatted =[]
		for row in results:
			formatted.append(dict(zip(columns, row)))


		self.saveCache(formatted,queryFile)
		return formatted
	"""
		Developer Time Map based on 30 days of activity.
		Developers that have contributed within 30 days of each other
		at the same project and made impactful commits.

		a_email,
		a_app,
		a_cwhen,
		a_csha,
		b_email,
		b_app,
		b_cwhen,
		b_csha
	"""
	def getDeveloperTimeMap(self):

		commitPairs=self.executeQueryWithCache('developerMap.sql')

		developerMap={}
		for pair in commitPairs:
			developerA=pair[0]
			developerB=pair[4]

			organization=pair[1].split("-")[0].lower()
			if developerA not in developerMap:
				developerMap[developerA]={}
				developerMap[developerA][developerB]=0
			elif(developerB not in developerMap[developerA]):
				developerMap[developerA][developerB]=0

			developerMap[developerA][developerB]+=1;


		return developerMap;
	"""
		Gets the quality metrics of a developer at the application level
		 email,
		 app,
		 loc,
		 cpx,
		 sml,
		 vul,
		 fbg,
		 locs_inc,
		 cplxs_inc,
		 smls_inc,
		 vuls_inc,
		 fbgs_inc,
		 locs_dec,
		 cplxs_dec,
		 smls_dec,
		 vuls_dec,
		 fbgs_dec,
		 total

	"""
	def getQuality(self):
		return self.executeQueryWithCache('increase_decrease_query.sql')
	"""
	Gets the compliation results for a developer at the application level
	 email,
	 app,
	 domain,
	 organization,
	 c_impactful,
	 c_analyzed

	"""
	def getCompilation(self):
		return self.executeQueryWithCache('compilation.sql')
	"""
		Gets the quality metrics AND compilation of a developer at the application level
		 email,
		 app,
		 loc,
		 cpx,
		 sml,
		 vul,
		 fbg,
		 locs_inc,
		 cplxs_inc,
		 smls_inc,
		 vuls_inc,
		 fbgs_inc,
		 locs_dec,
		 cplxs_dec,
		 smls_dec,
		 vuls_dec,
		 fbgs_dec,
		 total,
		 impactful,
		 analyzed
	"""
	def getQualityCompilation(self):
		return self.executeQueryWithCache('quality_compilation.sql')
	"""
	        locs.email         AS email,
	        locs.organization as organization,
	        locs.domain       as domain,
	        locs.app           as app,
	        locs.cwhen as cwhen,
	        locs.message as message,
	        --email_app_group_by_impactful.domain as domain,
	        --email_app_group_by_impactful.organization as organization,
	        --email_app_group_by_impactful.c_analyzed as c_analyzed,
	        --email_app_group_by_impactful.c_impactful as c_impactful,
	        locs.cnt           AS loc,
	        cplxs.cnt          AS cpx,
	        smls.cnt           AS sml,
	        vuls.cnt           AS vul,
	        fbgs.cnt           AS fbg,
	        locs_inc.cnt       AS locs_inc,
	        cplxs_inc.cnt      AS cplxs_inc,
	        smls_inc.cnt       AS smls_inc,
	        vuls_inc.cnt       AS vuls_inc,
	        fbgs_inc.cnt       AS fbgs_inc,
	        locs_dec.cnt       AS locs_dec,
	        cplxs_dec.cnt      AS cplxs_dec,
	        smls_dec.cnt       AS smls_dec,
	        vuls_dec.cnt       AS vuls_dec,
	        fbgs_dec.cnt       AS fbgs_dec
	"""
	def getQualityCompilationCommitLevel(self):

		results = self.executeQueryWithCache('quality_compilation_commit_level.sql')
		columns = [column[0] for column in self.cur.description]
		compilation=[]
		for row in results:
			compilation.append(dict(zip(columns, row)))

		return compilation

	"""
		Get Compilation at the organization level and affiliation status
							organization,
							'unaffiliated'                     as status,
							sum(c_analyzed) / sum(c_impactful) as comp_rate,

							sum(cnt_email)                     as developers,
							sum(c_analyzed)                    as c_analyzed,
							sum(c_impactful)                   as c_impactful
	"""
	def getAffiliationCompilation(self):
		return self.executeQueryWithCache('compilation_affiliation.sql')
	"""
		Get Quality Metrics at the organization level and affiliation status
	  	organization,
		'unaffiliated' as status,
		sum(loc)     as locs,
		sum(sml)     as smls,
		sum(cpx)     as cpxs,
		sum(vul)     as vuls,
		sum(fbg)     as fbgs,
		 sum(locs_inc)     as locs_inc,
		sum(smls_inc)     as smls_inc,
		sum(cplxs_inc)     as cpxs_inc,
		sum(vuls_inc)     as vuls_inc,
		sum(fbgs_inc)     as fbgs_inc,
		sum(locs_dec)     as locs_dec,
		sum(smls_dec)     as smls_dec,
		sum(cplxs_dec)     as cpxs_dec,
		sum(vuls_dec)     as vuls_dec,
		sum(fbgs_dec)     as fbgs_dec,
		sum(total)   as total,
	"""
	def getAffiliationQuality(self):
		results = self.executeQueryWithCache('increase_decrease_affiliation.sql')
		return results
