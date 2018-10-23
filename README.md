## SQUAAD ANALYSIS FRAMEWORK

## Dependencies

Install R on Ubuntu:
`sudo apt-get install r-base`

## Installation

* Download V1.0 
`https://github.com/fostiropoulos/squaad/releases/download/v1.0/squaad-1.0.tar.gz`

* To install the SQUAAD library
`pip install squaad-1.0.tar.gz`

## Usage

#### Creating new database connection
~~~~
myConnection=db("config.json","cache")
print("Connection Status: %s"%myConnection.testConnection())
~~~~

#### Config.json and Cache

* Config.json follows the following format:
~~~~
{"pgsql":{"host":"","user":"","passwd":"","db":""} }
~~~~
* Cache folder is used to save results of the queries and uses the cache next time you execute a query.

#### Games-Howell Statistics Test

~~~~
affiliation=myConnection.getAffiliationCompilation()
results={}

groups={}
for entry in affiliation:
    groups[entry[0]+"_"+entry[1]]={}
    groups[entry[0]+"_"+entry[1]][True]=int(entry[4])
    groups[entry[0]+"_"+entry[1]][False]=int(entry[5])-int(entry[4])


results["comp"]=stats.gamesHowellBinomial(groups)
~~~~~

#### Input Format of gamesHowellBinomial

~~~~
{"GROUP1":{True:100, False:3999}, "GROUP2":{True:2999,False:2939}}
~~~~

#### Output Format of gamesHowellBinomial

~~~~
{"GROUP1":{"GROUP2":p-value,"rate":0.xx}, "GROUP2":{"GROUP1":p-value,"rate":0.xx}
~~~~

#### Classification Pipeline with KFold Usage

Parameters

* `X` Pandas dataframe with set of data. Each column is a feature
* `Y`  Labels for the set of data.
* `split_columns` (Optional) **unimplemented**, columns to split by. That is columns that can have bias, we take into consideration during splitting
* `kfolds` (Optional)  number of folds to run.
* `classifiers` (Optional)  dictionary containing classifiers to use
* `balancers` (Optional)  the balancers you want to run

##### Classifiers

Default Value
~~~~

classifiers={"Nearest Neighbors":KNeighborsClassifier(3),
                        "Linear SVM":    SVC(kernel="linear", C=0.025),
                        "RBF SVM":    SVC(gamma=2, C=1),
                        "Gaussian Process":    GaussianProcessClassifier(1.0 * RBF(1.0)),
                        "Decision Tree" :    DecisionTreeClassifier(max_depth=5),
                        "Random Forest":    RandomForestClassifier(max_depth=5, n_estimators=10, max_features=1),
                        "Neural Net":    MLPClassifier(alpha=1),
                        "AdaBoost":    AdaBoostClassifier(),
                        "Naive Bayes":    GaussianNB(),
                        "QDA":QuadraticDiscriminantAnalysis()
                        }
~~~~

#### Balancers

Default Value
~~~~
balancers={
            "Unbalanced":None,
            "SMOTE":SMOTE(),
            "SMOTEEN":SMOTEENN(),
            "SMOTETomek":SMOTETomek(),
            "RandomUnderSampler":RandomUnderSampler()
        }
~~~~

If not want to run any balancing, use `balancers={ "Unbalanced":None}`

#### ML Pipeline examples

~~~~

dataset=myConnection.getQualityCompilation()
df = pd.DataFrame(dataset)
df['affiliation']= np.where(
		((df["domain"] == 'google.com') & (df["organization"]=='google')) |
		((df["domain"] == 'apache.org') & (df["organization"]=='apache')) |
		((df["domain"] == 'netflix.com') & (df["organization"]=='netflix'))
                ,df["organization"]+"-affiliated",df["organization"] +"-unaffiliated" )
X=df[['locs_inc', 'cplxs_inc', 'smls_inc', 'vuls_inc', 'fbgs_inc', 'locs_dec', 'cplxs_dec', 'smls_dec', 'vuls_dec', 'fbgs_dec']]
Y=df['affiliation']
mlPipeline.classificationPipeLineKfold(X,Y)
 ~~~~
