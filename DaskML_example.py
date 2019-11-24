# Note that this code must be run from python3 or ipython3 in a session's 
# terminal, not run directly in the graphical console. See
# https://github.com/dask/dask/issues/4612

import cdsw_dask_utils
import cdsw
import numpy as np
import pandas as pd

# Run a Dask cluster with three workers and return an object containing
# a description of the cluster. 
# 
# Note that the scheduler will run in the current session, and the Dask
# dashboard will become available in the nine-dot menu at the upper
# right corner of the CDSW app.

cluster = cdsw_dask_utils.run_dask_cluster(
  n=2, \
  cpu=1, \
  memory=2, \
  nvidia_gpu=0
)


# #1. Load the data (From File )
input_file = "data/WineNewGBTDataSet.csv"
col_Names=["fixedAcidity",
    "volatileAcidity",
    "citricAcid",
    "residualSugar",
    "chlorides",
    "freeSulfurDioxide",
    "totalSulfurDioxide",
    "density",
    "pH",
    "sulphates",
    "Alcohol",
    "Quality"]


wine_df = pd.read_csv(input_file,sep=";",header=None, names=col_Names)
wine_df.head()


# #### Cleanup - Remove invalid data
wine_df.Quality.replace('1',"Excellent",inplace=True)
print(wine_df.describe())


# # 2. Build a classification model using MLLib
# ## Step 1 Encode labels and split dataset into train and validation 

# ### encode labels 
wine_df.Quality = pd.Categorical(wine_df.Quality)
wine_df['Label'] = wine_df.Quality.cat.codes
wine_df.head()

# ### Split Test/Train
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(wine_df.iloc[:,:11],
                                                    wine_df['Label'], 
                                                    test_size=0.2, 
                                                    random_state=30)


# ## Step 2 : Prepare Classifier ( Random Forest in this case )
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV

# ### parameters for grid search
param_numTrees = list(range(10,50,5))
param_maxDepth = list(range(4,16,2))

rfc = RandomForestClassifier(random_state=10, n_jobs=-1)

GS_params = { 
    'n_estimators': param_numTrees,
    'max_depth' : param_maxDepth
}

CV_rfc = GridSearchCV(estimator=rfc, 
                      param_grid=GS_params, 
                      cv= 3,
                      verbose = 1,
                      n_jobs=-1
                     )

# ### Fit Model
print ("normal backend" )
CV_rfc.fit(X_train, y_train)

# ### Show Best Parameters 
print(CV_rfc.best_params_)

# ## Connect a Dask client to the scheduler address in the cluster
print("Dask Backend")

from dask.distributed import Client
client = Client(cluster["scheduler_address"])

import joblib
with joblib.parallel_backend('dask'):
  CV_rfc.fit(X_train, y_train)


# ### Show Best Parameters 
print(CV_rfc.best_params_)
