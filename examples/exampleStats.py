from squaad import stats
from squaad import file
from squaad import db

"""
For the config file and format please look the repository for an example
{"pgsql":{"host":"","user":"","passwd":"","db":""} }
Support only for postgresql. Database must have the tables and fields of
SQUAAD Database

@config_file path to config file that contains the connection information
@cache_folder path to the cache folder where to save the results
"""
myConnection=db("config.json","cache")

stats=stats()

affiliation=myConnection.getAffiliationCompilation()
results={}

groups={}
for entry in affiliation:
    affKey=entry['organization']+"_"+entry['status']
    groups[affKey]={}
    groups[affKey][True]=int(entry['c_impactful'])
    groups[affKey][False]=int(entry['c_impactful'])-int(entry['c_analyzed'])


results["comp"]=stats.gamesHowellBinomial(groups)

affiliation=myConnection.getAffiliationQuality()
#print(affiliation)
metrics= {'locs', 'smls', 'cpxs', 'vuls', 'fbgs', 'locs_inc', 'smls_inc', 'cpxs_inc', 'vuls_inc', 'fbgs_inc', 'locs_dec', 'smls_dec', 'cpxs_dec', 'vuls_dec', 'fbgs_dec'}
#CONVERT RESULTS TO ARRAY FOR PASSING TO STATS CHECK
for metric in metrics:

    groups={}
    for entry in affiliation:
        groups[entry["organization"]+"_"+entry["status"]]={}
        groups[entry["organization"]+"_"+entry["status"]][True]=int(entry[metric])
        groups[entry["organization"]+"_"+entry["status"]][False]=int(entry["total"])-int(entry[metric])

    results[metric]=stats.gamesHowellBinomial(groups)

file.saveResultsExcel(results,"stat.xls")
