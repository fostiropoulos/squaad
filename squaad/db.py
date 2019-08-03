import psycopg2
import datetime
import json
import sys
from squaad import file
import os

class db():
	"""Utilities and functions for retreiving specific type of data from the squaad Database. Makes working with the data easier and avoids re-writing queries. Also makes sure the queries used for the analysis are correct since they are validated.
	"""
	cur=None;

	cache_folder=None


	def __init__(self, config_file , cache_folder=None):
		"""Initialize the Database object that connects to the SQUAAD database. Please ensure you have the proper database formatself. Support only for postgresql.

		Example:
			myConnection=db("config.json","cache")
			# For the config file and format please look the repository for an example
		Args:
			config_file (str): path to config file that contains the connection information. Fromat, `{"pgsql":{"host":"","user":"","passwd":"","db":""}`
			cache_folder (:obj:`str`, optional): path to the cache folder. None for no cache.
		"""

		if(cache_folder!=None):
			if(not os.path.isdir(cache_folder)):
				raise Exception("Cache folder %s doesn't exist"%cache_folder)

		try:
			with open(config_file) as json_data_file:
				config = json.load(json_data_file)
		except Exception as e:
			print("Could not laod configuration file: "+config_file)
			raise e

		try:
			conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'"%(config["pgsql"]["db"],config["pgsql"]["user"],config["pgsql"]["host"],config["pgsql"]["passwd"],))
		except Exception as e:
			print ("I am unable to connect to the database")
			raise e
		self.cur = conn.cursor()
		self.testConnection()
		self.cache_folder=cache_folder

	def testConnection(self):
		"""Initialize the Database object that connects to the SQUAAD database. Please ensure you have the proper database formatself. Support only for postgresql.

		Example:
			myConnection.testConnection()
		"""
		try:
			self.cur.execute("SELECT 1")
			return True
		except Exception as e:
			print("Could not connect")
			raise e

	def getQuery(name):
		"""Helper function, reads sql file into a string
		Args:
			name (str): name of predefined SQL file to read. Stored internally
		"""
		with open(os.path.join(os.path.dirname(__file__), 'sql',name), 'r') as queryFile:
			query = queryFile.read()
		return query


	def readCache(self,name):
		"""If cache folder exists, load object from it
		Args:
			name (str): Cache file name
		Return:
			File object if succesful, None otherwise
		"""
		if(self.cache_folder!=None):
			return file.file.load_obj(os.path.join(self.cache_folder,name))
		return None

	def saveCache(self,data,name):
		"""If cache folder exists, save object to it
		Args:
			data (obj): Object to save on cache file
			name (str): Name of the cache file
		"""
		if(self.cache_folder!=None):
			file.file.save_obj(data,os.path.join(self.cache_folder,name))


	def executeQueryWithCache(self,queryFile):
		"""Executes query and checks if it is in cache.
		Args:
			queryFile (str): Executes a query file and saves results to cache
		"""
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

	def getDeveloperTimeMap(self):
		"""Developer Time Map based on 30 days of activity.	Developers that have contributed within 30 days of each other at the same project and made impactful commits.
		Returns:
			Dictionary with keys: a_email, a_app,	a_cwhen, a_csha, b_email, b_app, b_cwhen, b_csha
		"""
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

	def getQuality(self):
		"""Gets the quality metrics of a developer at the application level. Query: increase_decrease_query.sql
		Returns:
			Dictionary with keys: email,app,loc,cpx,sml,vul,fbg,locs_inc,cplxs_inc,smls_inc,vuls_inc,fbgs_inc,locs_dec,cplxs_dec,smls_dec,vuls_dec,fbgs_dec,total
		"""
		return self.executeQueryWithCache('increase_decrease_query.sql')

	def getCompilation(self):
		"""Gets the compliation results for a developer at the application level. Query: compilation.sql
		Returns:
			Dictionary with keys: email, app, domain, organization, c_impactful, c_analyzed
		"""
		return self.executeQueryWithCache('compilation.sql')

	def getQualityCompilation(self):
		"""Gets the quality metrics AND compilation of a developer at the application level. Query: quality_compilation.sql
		Returns:
			Dictionary with keys: email, app, loc, cpx, sml, vul, fbg, locs_inc, cplxs_inc, smls_inc, vuls_inc, fbgs_inc, locs_dec, cplxs_dec, smls_dec, vuls_dec, fbgs_dec, total, impactful, analyzed
		"""
		return self.executeQueryWithCache('quality_compilation.sql')

	def getQualityCompilationCommitLevel(self):
		"""Get Quality metrics at the commit level. Query: quality_compilation_commit_level.sql
		Returns:
			Dictionary with keys: email, organization, domain, app,cwhen, message, loc, cpx, sml, vul, fbg, locs_inc, cplxs_inc, smls_inc, vuls_inc, fbgs_inc, locs_dec, cplxs_dec, smls_dec, vuls_dec, fbgs_dec
		"""
		return self.executeQueryWithCache('quality_compilation_commit_level.sql')


	def getAffiliationCompilation(self):
		"""Get Compilation at the organization level and affiliation status. Query: compilation_affiliation.sql
		Returns:
			Dictionary with keys: organization, status, comp_rate, developers, c_analyzed, c_impactful
		"""
		return self.executeQueryWithCache('compilation_affiliation.sql')

	def getAffiliationQuality(self):
		"""Get Quality Metrics at the organization level and affiliation status. Query: increase_decrease_affiliation.sql
		Returns:
			Dictionary with keys: organization, status, locs, smls, cpxs, vuls, fbgs, locs_inc, smls_inc, cpxs_inc, vuls_inc, fbgs_inc, locs_dec, smls_dec, cpxs_dec, vuls_dec, fbgs_dec, total
		"""
		results = self.executeQueryWithCache('increase_decrease_affiliation.sql')
		return results

	def getQualityDifferenceCommitLevel(self):
		"""Gets commit level the quality metrics. Sparse results. Query: quality_difference_commit_level.sql
		Returns:
			Dictionary with keys: csha,application,cwhen,message,branch,email,csha,classes,comment_lines_density,vulnerabilities,lines,ncloc,complexity,security_rating,major_violations,duplicated_blocks,code_smells,file_complexity,functions,duplicated_files,duplicated_lines_density,reliability_rating,critical_violations,violations,statements,blocker_violations,reliability_remediation_effort,duplicated_lines,bugs,security_remediation_effort,directories,info_violations,sqale_index,sqale_debt_ratio,minor_violations,files,sqale_rating,csha,basic,emptycode,cloneimplementation,comments,codesize,stringandstringbuffer,naming,strictexceptions,optimization,design,securitycodeguidelines,braces,typeresolution,coupling,finalizer,importstatements,unusedcode,unnecessary,csha,security,bad_practice,malicious_code,performance,correctness,style,experimental,mt_correctness,i18n
		"""
		results = self.executeQueryWithCache('quality_difference_commit_level.sql')
		return results
