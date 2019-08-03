from pingouin import pairwise_gameshowell

class stats():


	def gamesHowellBinomial(self, groups):
		"""Compares groups with their occurances, works for observable discrete events.	E.g. "How many times event A occured." Groups, list of classes that are potential outcomes values, their observations
		Args:
			groups(obj): Dictionary of groups
		Example:
			stats.gamesHowellBinomial({"group":{True:1000,False:50},"group2":{True:10020,False:5220}})
		"""
		groupNames=[]
		groupValues=[]
		for group in groups:
			groupNames+=int(groups[group][True]+groups[group][False])*[group]
			groupValues+=int(groups[group][True])*[1]+int(groups[group][False])*[0]

		df=pd.DataFrame()
		df['group_names']=groupNames
		df['group_values']=groupValues

		return pairwise_gameshowell(dv='group_values', between='group_names', data=df)
