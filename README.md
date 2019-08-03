# SQUAAD ANALYSIS FRAMEWORK

## Installation

`pip install squaad`

### Releases

* V2.0 `https://github.com/fostiropoulos/squaad/releases/download/v2.0/squaad-2.0.tar.gz`


### Install from Binary
`pip install squaad-2.0.tar.gz`

## Usage

### Creating new database connection
~~~~
myConnection=db("config.json","cache")
print("Connection Status: %s"%myConnection.testConnection())
~~~~

### Config.json and Cache

* Config.json follows the following format:
~~~~
{"pgsql":{"host":"","user":"","passwd":"","db":""} }
~~~~
* Cache folder is used to save results of the queries and uses the cache next time you execute a query.

### Games-Howell Statistics Test

~~~~
stats.gamesHowellBinomial({"GROUP1":{True:100, False:3999}, "GROUP2":{True:2999,False:2939}})
~~~~~

### Classification Pipeline with KFold Usage

Parameters

* `X` Pandas dataframe with set of data. Each column is a feature
* `Y`  Labels for the set of data.
* `split_columns` (Optional) **unimplemented**, columns to split by. That is columns that can have bias, we take into consideration during splitting
* `kfolds` (Optional)  number of folds to run.
* `classifiers` (Optional)  dictionary containing classifiers to use
* `balancers` (Optional)  the balancers you want to run

### Classifiers

Default Classifiers:
* Nearest Neighbors
* Linear SVM
* RBF SVM
* Gaussian Process
* Decision Tree
* Random Forest
* Neural Net
* AdaBoost
* Naive Bayes
* QDA

### Balancers

Default Classifiers:
* Unbalanced
* SMOTE
* SMOTEEN
* SMOTETomek
* RandomUnderSampler

### ML Pipeline examples

~~~~
X=df[['locs_inc', 'cplxs_inc', 'smls_inc', 'vuls_inc', 'fbgs_inc', 'locs_dec', 'cplxs_dec', 'smls_dec', 'vuls_dec', 'fbgs_dec']]
Y=df['affiliation']
mlPipeline.classificationPipeLineKfold(X,Y)
~~~~
