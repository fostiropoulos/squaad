from rpy2.robjects.packages import importr
import rpy2.robjects as robjects

class stats():
	def __init__(self):
		base = importr('base')

		robjects.r('''
				games.howell <- function(grp, obs) {

				  #Create combinations
				  combs <- combn(unique(grp), 2)

				  # Statistics that will be used throughout the calculations:
				  # n = sample size of each group
				  # groups = number of groups in data
				  # Mean = means of each group sample
				  # std = variance of each group sample
				  n <- tapply(obs, grp, length)
				  groups <- length(tapply(obs, grp, length))
				  Mean <- tapply(obs, grp, mean)
				  std <- tapply(obs, grp, var)

				  statistics <- lapply(1:ncol(combs), function(x) {

					mean.diff <- Mean[combs[2,x]] - Mean[combs[1,x]]

					#t-values
					t <- abs(Mean[combs[1,x]] - Mean[combs[2,x]]) / sqrt((std[combs[1,x]] / n[combs[1,x]]) + (std[combs[2,x]] / n[combs[2,x]]))

					# Degrees of Freedom
					df <- (std[combs[1,x]] / n[combs[1,x]] + std[combs[2,x]] / n[combs[2,x]])^2 / # Numerator Degrees of Freedom
					  ((std[combs[1,x]] / n[combs[1,x]])^2 / (n[combs[1,x]] - 1) + # Part 1 of Denominator Degrees of Freedom
						 (std[combs[2,x]] / n[combs[2,x]])^2 / (n[combs[2,x]] - 1)) # Part 2 of Denominator Degrees of Freedom

					#p-values
					p <- ptukey(t * sqrt(2), groups, df, lower.tail = FALSE)

					# Sigma standard error
					se <- sqrt(0.5 * (std[combs[1,x]] / n[combs[1,x]] + std[combs[2,x]] / n[combs[2,x]]))

					# Upper Confidence Limit
					upper.conf <- lapply(1:ncol(combs), function(x) {
					  mean.diff + qtukey(p = 0.95, nmeans = groups, df = df) * se
					})[[1]]

					# Lower Confidence Limit
					lower.conf <- lapply(1:ncol(combs), function(x) {
					  mean.diff - qtukey(p = 0.95, nmeans = groups, df = df) * se
					})[[1]]

					# Group Combinations
					grp.comb <- paste(combs[1,x], ':', combs[2,x])

					# Collect all statistics into list
					stats <- list(grp.comb, mean.diff, se, t, df, p, upper.conf, lower.conf)
				  })

				  # Unlist statistics collected earlier
				  stats.unlisted <- lapply(statistics, function(x) {
					unlist(x)
				  })

				  # Create dataframe from flattened list
				  results <- data.frame(matrix(unlist(stats.unlisted), nrow = length(stats.unlisted), byrow=TRUE))

				  # Select columns set as factors that should be numeric and change with as.numeric
				  results[c(2, 3:ncol(results))] <- round(as.numeric(as.matrix(results[c(2, 3:ncol(results))])), digits = 3)

				  # Rename data frame columns
				  colnames(results) <- c('groups', 'Mean Difference', 'Standard Error', 't', 'df', 'p', 'upper limit', 'lower limit')

				  return(results)
				}

				''')


		self.gh = robjects.globalenv['games.howell']


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

		results=self.gh(robjects.StrVector(groupNames),robjects.IntVector(groupValues) )
		tableDict=stats.rTableToDict(results)
		for group in groups:
			tableDict[group]['rate']=groups[group][True]/(groups[group][False]+groups[group][True])
		return tableDict



	def rTableToDict(results):
		"""Convert r results into a python dictionary format
		Args:
			results(obj): r object results in table format
		Example:
			stats.rTableToDict(self.gh(robjects.StrVector(groupNames),robjects.IntVector(groupValues)))
		"""
		tableDict={}
		for i in range(len(results[0])):
			firstQuant=str(results.rx(i+1,True)[0]).split("\n")[0].split(":")[0].split(' ')[1]
			secondQuant=str(results.rx(i+1,True)[0]).split("\n")[0].split(": ")[1]
			#print(orgOne)
			#print(orgTwo)
			pValue=str(results.rx(i+1,True)[5]).split(' ')[1].split("\n")[0];
			if(firstQuant not in tableDict):
				tableDict[firstQuant]={}
			if(secondQuant not in tableDict):
				tableDict[secondQuant]={}

			tableDict[firstQuant][secondQuant]=pValue
			tableDict[secondQuant][firstQuant]=pValue
		#print(tableDict)
		return tableDict;
