import sys
import os
import pickle
import xlwt

"""

File helper functions to help generate reports, serialize files and more.

"""
class file():

	def saveResultsExcel(results,path):
		book = xlwt.Workbook()
		sh = book.add_sheet("output")
		n=1
		print(results)

		print(results.keys())
		for key in sorted(results.keys()):
			sh.write(n,0,key)
			for key2 in sorted(results[key].keys()):
				sh.write(n,1,key2)
				#sh.write(n,2+len(sorted(results[key].keys())),str(metricValues[key][key2]["value"]))
				#sh.write(n,3+len(sorted(results[key].keys())),str(metricValues[key][key2]["total"]))

				i=2
				for key3 in sorted(set(list(results[key][key2].keys())+list(results[key].keys()))):
					if(key3 not in results[key][key2].keys()):
						sh.write(n,i,"-")

					else:
						sh.write(n,i,results[key][key2][key3])
					i+=1
				n+=1

		i=2
		# WRITE HEADER
		for key in sorted(results.keys()):
			for key2 in sorted(results[key].keys()):
				for key3 in sorted(set(list(results[key][key2].keys())+list(results[key].keys()))):

					sh.write(0,i,key3)
					i+=1
				break
			break
		book.save(path)



	@staticmethod
	def load_obj(name):
		if not os.path.isfile(name):
			return None
		with open(name, 'rb') as f:
			return pickle.load(f)

	def save_obj(obj, name ):
		with open(name , 'wb') as f:
			pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
