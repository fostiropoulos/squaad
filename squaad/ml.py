from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.gaussian_process import GaussianProcessClassifier
from sklearn.gaussian_process.kernels import RBF
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis
from imblearn.under_sampling import RandomUnderSampler
from imblearn.over_sampling import RandomOverSampler
from imblearn.over_sampling import SMOTE
from imblearn.combine import SMOTEENN, SMOTETomek
import pandas as pd
import numpy as np
import seaborn as sns
from sklearn.model_selection import KFold
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.metrics import accuracy_score, roc_auc_score, precision_score, recall_score, f1_score
import os
import time
import matplotlib.pyplot as plt

class ml():
    """Machine Learning Pipeline Class
    """

    def __init__(self, outputFolder=None,output=False,debug=False):
        """Constructor for pipeline.

        Example:
            mlPipeline=ml(output=True, outputFolder="cache")
        Args:
            outputFolder (str): Dictionary 2d, example[A][B]=example[B][A]
            path (str): path to the Excel file to be saved
        """
        self.outputFolder=outputFolder
        if(outputFolder!=None):
            if(not os.path.isdir(outputFolder)):
                raise Exception("Output folder %s doesn't exist"%outputFolder)
        self.output=output
        if(self.output):
            self.output=True
        else:
            self.output=False
        self.debug=debug

    def classificationPipeLineKfold(self, X,Y, split_columns=None, kfolds=10,
                                        classifiers={"Nearest Neighbors":KNeighborsClassifier(3),
                                                        "Linear SVM":    SVC(kernel="linear", C=0.025),
                                                        "RBF SVM":    SVC(gamma=2, C=1),
                                                        "Gaussian Process":    GaussianProcessClassifier(1.0 * RBF(1.0)),
                                                        "Decision Tree" :    DecisionTreeClassifier(max_depth=5),
                                                        "Random Forest":    RandomForestClassifier(max_depth=5, n_estimators=10, max_features=1),
                                                        "Neural Net":    MLPClassifier(alpha=1),
                                                        "AdaBoost":    AdaBoostClassifier(),
                                                        "Naive Bayes":    GaussianNB(),
                                                        "QDA":QuadraticDiscriminantAnalysis()},
                                        balancers={"Unbalanced":None,
                                                    "SMOTE":SMOTE(),
                                                    "SMOTEEN":SMOTEENN(),
                                                    "SMOTETomek":SMOTETomek(),
                                                    "RandomUnderSampler":RandomUnderSampler()}):
        """Classification pipeline, with balancing of data and cross validation.
        Args:
            X(str): Pandas dataframe with set of data.
            Y(str): Labels for the set of data.
            split_columns(array): unimplemented, columns to split by. That is columns that can have bias, we take into consideration during splitting
            kfolds(int): number of folds to run.
            classifiers(obj): dictionary of classifiers. the classifiers you want to run, at least one must be present
            balancers(obj): Dictionary of Balancers to run, set None for no balancers and/or "Unbalanced":None to include unbalanced data in your analysis
        """

        # TODO feature weights
        scores_aff={}

        dataset=X
        # TODO How to get multiple columns from the split column list all their unique combinations
        if(split_columns!=None):
            for column in split_columns:
                dataset=dataset[column].unique()

        kf = KFold(n_splits=kfolds, shuffle=True)
        fold=0;
        foldid = {}
        totacc = {}

        ytlog = {}
        yplog = {}
        for name in classifiers:


            foldid[name]={}
            totacc[name]={}
            ytlog[name]={}
            yplog[name]={}
            for balancer in balancers:
                foldid[name][balancer]=0.
                totacc[name][balancer]=0.
                ytlog[name][balancer]=[]
                yplog[name][balancer]=[]

        for train_index, test_index in kf.split(dataset):
            #foldid += 1
            X_train=None
            y_train=None
            X_test=None
            y_test=None
            unique, counts = np.unique(Y, return_counts=True)
            xAxis="Before Spliting the Data"
            tab = pd.DataFrame({
                xAxis: unique.tolist(),
                'Frequency': counts.tolist()
            })
            sns.barplot(x=xAxis, y='Frequency', data=tab)
            if(self.outputFolder!=None):
                plt.savefig(os.path.join(self.outputFolder,"%d"%int(time.time())))
            if(self.output and fold==0):
                plt.show()
            if(split_columns!=None):

                frames_X_train=[]
                frames_y_train=[]
                frames_X_test=[]
                frames_y_test=[]
                for entry in np.nditer(dataset[train_index]):
                    #dev=dev.astype(int)
                    index=np.asscalar(dev)
                    # TODO Figure out how to aggregate split columns
                    frames_X_train.append(X.loc[X['email_index'] == index])
                    frames_y_train.append(Y.loc[Y['email_index'] == index])

                for entry in np.nditer(dataset[test_index]):
                    #dev=dev.astype(int)
                    index=np.asscalar(dev)
                    frames_X_test.append(X.loc[X['email_index'] == index])
                    frames_y_test.append(Y.loc[Y['email_index'] == index])
                X_train=pd.concat(frames_X_train)
                y_train=pd.concat(frames_y_train)
                X_test=pd.concat(frames_X_test)
                y_test=pd.concat(frames_y_test)
            else:
                X_train=X.values[train_index]
                y_train=Y.values[train_index]
                X_test=X.values[test_index]
                y_test=Y.values[test_index]

                unique, counts = np.unique(y_train, return_counts=True)
                xAxis="Fold %d Train Set"%fold
                tab = pd.DataFrame({
                    xAxis: unique.tolist(),
                    'Frequency': counts.tolist()
                })
                sns.barplot(x=xAxis, y='Frequency', data=tab)
                if(self.outputFolder!=None):
                    plt.savefig(os.path.join(self.outputFolder,"%d"%int(time.time())))
                if(self.output and fold==0):
                    plt.show()
            for balancer, rus in balancers.items():
                if(rus!=None):
                    X_train, y_train = rus.fit_sample(X_train, y_train)

                if(fold==0):
                    unique, counts = np.unique(y_train, return_counts=True)
                    xAxis="Fold %d using %s on %s"%(fold,name,balancer)
                    tab = pd.DataFrame({
                        xAxis: unique.tolist(),
                        'Frequency': counts.tolist()
                    })
                    sns.barplot(x=xAxis, y='Frequency', data=tab)
                    if(self.outputFolder!=None):
                        plt.savefig(os.path.join(self.outputFolder,"%d"%int(time.time())))
                    if(self.output and self.debug):
                        plt.show()
                #scores_aff[organization]={}
                for name in classifiers:
                    clf=classifiers[name]
                    clf.fit(X_train, y_train)
                    y_pred = clf.predict(X_test)

                    acc = accuracy_score(y_pred, y_test)
                    totacc[name][balancer] += acc
                    ytlog[name][balancer] += list(y_test)
                    yplog[name][balancer] += list(y_pred)
                    if(self.output and self.debug):
                        print('\t%s Accuracy at Fold %d with %s:'%(name,fold,balancer), acc)
                        print(classification_report(y_test, y_pred))
            fold+=1
        for name in classifiers:
            for balancer, rus in balancers.items():
                if(self.output):
                    print("%s Average Accuracy: %0.3f" % (name, totacc[name][balancer] / kfolds))
                    print(classification_report(ytlog[name][balancer], yplog[name][balancer]))
        #TODO Add AUC ROC graph
