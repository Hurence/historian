---
layout: page 
title: Low Carbon Historian
---

## Goal

## Features

- Real-Time Analytics
- Energy consumtion Reports
- GDPR Friendly
- Performance & Optimization


## Green Performance indicators

- `EnergyImpactInKwh` / (overall, page, country, device_category)
- `Co2EqInKg` / (overall, page, country, device_category)
- 

## User stories

- web site energy footprint

## Energy impact computation through the `One byte model`

The energy impact of one action will be computed with the following formula :

    EnergyImpactInKwh = timeSpentOnActionInMin * deviceImpact + dataSizeInByte * (dataCenterImpact + networkImpact)

So we need to know :

- time spent on the action
- the type of device
- the data size involved in the action

According to
the [1byteModel](https://theshiftproject.org/wp-content/uploads/2019/10/Lean-ICT-Materials-Liens-%C3%A0-t%C3%A9l%C3%A9charger-r%C3%A9par%C3%A9-le-29-10-2019.pdf)
given by TheshiftProject

```java
public class OneByteModelEnergyImpact {
    // Energy Impact due to Data Centers (in kWh/byte)
    public static double ENERGY_IMPACT_DUE_TO_DATACENTERS = 7.2E-11;

    // Energy impact for FAN Wired (in kWh/byte)
    public static double ENERGY_IMPACT_FOR_FAN_WIRED = 4.29E-10;

    // Energy impact for FAN WIFI (in kWh/byte)
    public static double ENERGY_IMPACT_FOR_FAN_WIFI = 1.52E-10;

    // Energy impact for Mobile Network (in kWh/byte)
    public static double ENERGY_IMPACT_FOR_MOBILE_NETWORK = 8.84E-10;

    // Energy impact for device Smartphone (in kWh/min)
    public static double ENERGY_IMPACT_DEVICE_SMARTPHONE = 1.1E-4;

    // Energy impact for Mobile Network (in kWh/min)
    public static double ENERGY_IMPACT_DEVICE_LAPTOP = 3.2E-4;
}
```

Then the `EnergyImpactInKwh` can be correlated with `CarbonIntensityFactors` from geolocation to infer CO2 emissions :

```java
// in kgCO2e/kWh from LeanICT REN
public class CarbonIntensityFactors {
    // European Union
    public static double CARBON_INTENSITY_FACTORS_EU = 0.276;
    // United States
    public static double CARBON_INTENSITY_FACTORS_US = 0.493;
    // China
    public static double CARBON_INTENSITY_FACTORS_CH = 0.681;
    // World
    public static double CARBON_INTENSITY_FACTORS_WORLD = 0.519;
    // France
    public static double CARBON_INTENSITY_FACTORS_FR = 0.035;
}
```

## Data model

What we need to compute is an `EnergyImpactMetric`

```java
public class WebAppCrawlingSettings {
    private String webAppName;
    private String jsonKeyFile;
}
```



```java
public class EnergyImpactMetric {
    private String webAppName;
    private String country;
    private String pagePath;
    private String day;
    private String deviceCategory;     // loaded from GA
    private long pageViews;            // loaded from GA
    private int avgTimeOnPageInMs;     // loaded from GA
    private long pageSizeInBytes;      // loaded from advertools
    private double energyImpactInKwh;  // computed from 1byteModel
    private double co2EqInG;           // computed from CarbonIntensityFactors
}
```

What will be stored in historian as a daily measure :

```json
{
  "name": "energy_impact_in_kwh",
  "tags": "app_name,page_path,country"
}
```

## Crawling your website to get page sizes

With `advertools` python package we can crawl a website and get page sizes.

> see [scrappy settings](https://docs.scrapy.org/en/latest/topics/settings.html) for authentification settings

```python
import pandas as pd
from advertools import crawl

crawl('http://hurence.com', 'hurence.jl', follow_links=True)
enczp = pd.read_json('hurence.jl', lines=True)

enczp.columns
y = enczp[['url', 'size']]

```

- [page size](https://www.holisticseo.digital/python-seo/crawl-analyse-website/#how-to-see-average-and-general-size-of-web-pages-of-a-web-site-via-python)
- [advertools](https://advertools.readthedocs.io/en/master/index.html)

## Getting web (google) analytics insights

The reporting API from google analytics should be polled regularly to get pivot tables containing page views and average browsing time


    POST https://analyticsreporting.googleapis.com/v4/reports:batchGet

```json
{
  "reportRequests": [
    {
      "viewId": "196132511",
      "metrics": [
        {
          "expression": "ga:pageviews"
        },
        {
          "expression": "ga:avgTimeOnPage"
        }
      ],
      "includeEmptyRows": false,
      "dimensions": [
        {
          "name": "ga:deviceCategory"
        },
        {
          "name": "ga:country"
        },
        {
          "name": "ga:pagePath"
        }
      ],
      "pivots": [
        {
          "dimensions": [
            {
              "name": "ga:pagePath"
            }
          ],
          "metrics": [
            {
              "expression": "ga:avgTimeOnPage"
            }
          ]
        }
      ]
    }
  ]
}

```

```json
{
  "reports": [
    {
      "columnHeader": {
        "dimensions": [
          "ga:deviceCategory",
          "ga:country",
          "ga:pagePath"
        ],
        "metricHeader": {
          "metricHeaderEntries": [
            {
              "name": "ga:pageviews",
              "type": "INTEGER"
            },
            {
              "name": "ga:avgTimeOnPage",
              "type": "TIME"
            }
          ]
        }
      },
      "data": {
        "rows": [
          {
            "dimensions": [
              "desktop",
              "France",
              "/en/training/data-ingestion-analytics-streaming/real-time-processing-with-spark-streaming/"
            ],
            "metrics": [
              {
                "values": [
                  "1",
                  "0.0"
                ]
              }
            ]
          },
          {
            "dimensions": [
              "desktop",
              "France",
              "/fr-consulting/"
            ],
            "metrics": [
              {
                "values": [
                  "1",
                  "51.0"
                ]
              }
            ]
          }
        ],
        "rowCount": 99,
        "minimums": [
          {
            "values": [
              "1",
              "0.0"
            ]
          }
        ],
        "maximums": [
          {
            "values": [
              "27",
              "1638.0"
            ]
          }
        ]
      }
    }
  ]
}
```

=> overall application energy impact

dimensions: ga:deviceCategory, ga:country, ga:pagePath metrics: ga:pageviews, ga:avgTimeOnPage

https://ga-dev-tools.web.app/request-composer/


## Deploiement and orchestration

components

- historian (solr/server)
- web

## Issues

- new component : greensights-server
- add user regi


- fix log4j vulnerability
- [greensights-server] implement energy computation endpoints
- site crawler job as docker container
- google analytics job 

 

# Phase 2 :  IT monitoring


We can use `turbostat`under linux to get accurate watt consumtion for both cpu and ram associated with a given workload.

```bash
sudo turbostat -q -c package --num_iterations 1 --interval 1.000000 --show PkgWatt,RAMWatt,PkgTmp

# PkgTmp    PkgWatt	RAMWatt
# 29	    25.50	6.75
# 29	    25.51	6.75
```


scaphandre setup on hosts and kubernetes clusters

historian that scrape all these metrics

curl 'http://prometheus.monitoring.sandbox2.hurence.net/api/v1/query?query=up&time=2015-07-01T20:10:51.781Z'



KPI

top ten apps cpu & memory  KFlops / Wh
categorize apps => cpu intensive / ram intensive / both
infer app name from command line



