from squaad import ml
import pandas as pd
import numpy as np
from squaad import db



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
    mlPipeline.classificationPipeLineKfold(X,Y)
