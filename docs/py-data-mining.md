---
layout: page
title: Data Minig with python
---




- via Pandas
- et utiliser Dask et Cuda

### Lire avec pandas
Pour exploiter les données via cuda

```python
import dask
import pandas as pd
import numpy as np
import sys
from io import StringIO    
import requests

url = "http://islin-hdpledg01:8083/api/historian/v0/export/csv'"

payload = """
{
  "range": {
    "from": "2019-11-26T21:11:50.000Z",
    "to": "2019-11-29T20:17:04.000Z"
  },
  "targets": [
    {
      "target": "ack",
      "type": "timeserie"
    }
  ],
  "format": "csv",
  "maxDataPoints": 10000
}"""
headers = {
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data = payload)

TESTDATA = StringIO(response.text)

df = pd.read_csv(TESTDATA, sep=",")
df['timestamp'] = df['date']
df['date'] = pd.to_datetime(df['date'], unit='ms')
df.index = df['date']



>>> values=df['value']

>>> values.mean()
22.064017518948535

>>> values.asfreq('1H', method='bfill')
date
2017-11-19 14:24:01    22.210605
2017-11-19 15:24:01    22.259716
2017-11-19 16:24:01    22.200816
2017-11-19 17:24:01    22.289975
2017-11-19 18:24:01    21.887082
                         ...
2017-11-30 19:24:01    21.734108
2017-11-30 20:24:01    21.786581
2017-11-30 21:24:01    21.730731
2017-11-30 22:24:01    21.722667
2017-11-30 23:24:01    21.724261
Freq: H, Name: value, Length: 274, dtype: float64

```


### Dask, Cuda et GPU
La version Dask et cuda

```python
import numpy as np
import pandas as pd
import cudf
import dask_cudf

np.random.seed(12)


cu_df = cudf.DataFrame.from_pandas(df)


>>> cu_df = cudf.DataFrame.from_pandas(df)
>>> cu_df
                        metric      value                date      timestamp
date
2017-11-19 14:24:01  1C12.TE07  22.210605 2017-11-19 14:24:01  1511101441000
2017-11-19 14:26:00  1C12.TE07  22.283563 2017-11-19 14:26:00  1511101560000
2017-11-19 14:28:01  1C12.TE07  22.312486 2017-11-19 14:28:01  1511101681000
2017-11-19 14:30:01  1C12.TE07  22.366689 2017-11-19 14:30:01  1511101801000
2017-11-19 14:32:00  1C12.TE07  22.440140 2017-11-19 14:32:00  1511101920000
...                        ...        ...                 ...            ...
2017-11-30 23:50:01  1C12.TE07  21.744959 2017-11-30 23:50:01  1512085801000
2017-11-30 23:52:01  1C12.TE07  21.722180 2017-11-30 23:52:01  1512085921000
2017-11-30 23:54:01  1C12.TE07  21.639489 2017-11-30 23:54:01  1512086041000
2017-11-30 23:56:01  1C12.TE07  21.566633 2017-11-30 23:56:01  1512086161000
2017-11-30 23:58:01  1C12.TE07  21.593067 2017-11-30 23:58:01  1512086281000

[4849 rows x 4 columns]



>>> cu_df.value.mean()
22.064017518948535


By providing an integer each column is rounded to the same number of decimal places

>>> cu_df.value.round(1).value_counts()
22.1    749
22.0    700
21.7    693
22.4    622
22.3    618
21.9    426
22.2    406
21.8    332
21.6    139
22.5    134
22.8     10
22.7      6
22.6      5
22.9      5
21.2      2
21.3      1
23.0      1
Name: value, dtype: int32


# get all value before a given date
import datetime as dt
search_date = dt.datetime.strptime('2017-11-20', '%Y-%m-%d')
cu_df.query('date <= @search_date')

msk = np.random.rand(len(df)) < 0.8
train = cu_df[msk]
test = cu_df[~msk]


# Setup and fit clusters
from cuml.cluster import DBSCAN
dbscan_float = DBSCAN(eps=1.0, min_samples=1)
dbscan_float.fit(train['value'].round(1))

print(dbscan_float.labels_)

```


https://rapidsai.github.io/projects/cuml/en/latest/api.html#





## Requetes via Grafana
Le meilleur moyen de visualiser les données historian est via Grafana.

Une installation est disponible sur le serveur suivant : `http://islin-hdpledg01:3002`

user: admin
pswd: PNJnzmwZxwqOsItifEKP

### Ajout d'un dashboard

Cliquez sur `Create > Dashboard > Add query`

![](grafana-add-dash.png)

Selectionnez la datasource Simplejson précédemment créée à côté du Champs Query

![](grafana-select-ds.png)

Rentrez le nom de la timeserie que vous voulez observer, comme par ex `U312.C4TC04_PV.F_CV`.

Selectionnez le time range souhaité.

Sauvegardez et c'est pret !

![](grafana-select-ts.png)








