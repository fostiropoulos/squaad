import sys
import os
import pickle
import xlwt


class file():
	"""File helper functions to help generate reports, serialize files and more.
	"""
	@staticmethod
	def saveResultsExcel(results,path):
		"""Save results in Excel file, dictionary 2d

		Example:
			file.saveResultsExcel(results,"stat.xls")
		Args:
			results (obj): Dictionary 2d, example[A][B]=example[B][A]
			path (str): path to the Excel file to be saved
		"""

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
	def load_obj(path):
		"""Load pickle object
		Example:
			file.load_obj("test")
		Args:
			path (str): file path to pickle object
		"""
		if not os.path.isfile(path):
			return None
		with open(path, 'rb') as f:
			return pickle.load(f)

	@staticmethod
	def save_obj(obj, path ):
		"""Save pickle object
		Example:
			file.save_obj(dict,"test")
		Args:
			path (str): file path to pickle object
		"""
		with open(path , 'wb') as f:
			pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
