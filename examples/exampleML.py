from squaad import ml
import pandas as pd
import numpy as np
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

dataset=myConnection.getQualityCompilation()
df = pd.DataFrame(dataset)
df = df.drop(df[df['impactful'] < 2].index)
#create label
df['affiliation']= np.where(((df["domain"] == 'google.com') & (df["organization"]=='google')) |
								((df["domain"] == 'apache.org') & (df["organization"]=='apache')) |
								((df["domain"] == 'netflix.com') & (df["organization"]=='netflix'))
								,df["organization"]+"-affiliated",df["organization"] +"-unaffiliated"  )

metrics=['loc', 'cpx', 'sml', 'vul', 'fbg', 'locs_inc', 'cplxs_inc', 'smls_inc', 'vuls_inc', 'fbgs_inc', 'locs_dec', 'cplxs_dec', 'smls_dec', 'vuls_dec', 'fbgs_dec']


for metric in metrics:
	df[metric]=df[metric]/df['total']
df['analyzed']=df['analyzed']/df['impactful']

organizations=["google","apache","netflix"]
scores_aff={}

mlPipeline=ml(output=True, outputFolder="cache")

for organization in organizations:
    df_aff=df[(df['affiliation'] == organization+'-affiliated') | (df['affiliation'] == organization+'-unaffiliated' ) ]
    X=df_aff[['locs_inc', 'cplxs_inc', 'smls_inc', 'vuls_inc', 'fbgs_inc', 'locs_dec', 'cplxs_dec', 'smls_dec', 'vuls_dec', 'fbgs_dec']]
    Y=df_aff['affiliation']

    """
        The goal of this function is to make it easy for someone with no experience in ML, to run a pipeline,
        based on the SQUAAD data. This is a very limited function, please consider understanding your data better
        and running and running the correct analysis, pre-processing.

        X = Pandas dataframe with set of data.
        Y = Labels for the set of data.
        split_columns unimplemented, columns to split by. That is columns that can have bias, we take into consideration during splitting
        kfolds=10, number of folds to run.
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
                        },
        the classifiers you want to run, at least one must be present
        balancers={
            "Unbalanced":None,
            "SMOTE":SMOTE(),
            "SMOTEEN":SMOTEENN(),
            "SMOTETomek":SMOTETomek(),
            "RandomUnderSampler":RandomUnderSampler()
        }
        the balancers you want to run, at least one must be present. If not want to run any balancing, use only "Unbalanced":None,

        example:

        classificationPipeLineKfold(self,
                X,Y, split_columns=None, kfolds=10,
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
                                },
                                balancers={
                                    "Unbalanced":None,
                                    "SMOTE":SMOTE(),
                                    "SMOTEEN":SMOTEENN(),
                                    "SMOTETomek":SMOTETomek(),
                                    "RandomUnderSampler":RandomUnderSampler()
                                }
                                )
    """
    mlPipeline.classificationPipeLineKfold(X,Y)
