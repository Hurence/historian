---
layout: page
title: Web site energy consumption measurement
---


**Priority** : Medium

**Story Points** : 8


## Story

*As a* web application owner,
*I want* to measure the total energy consumption and carbon footprint of a given website during a time period
*so that* I can track Green Performance Indicators of my apps during time and compare to others .

## Acceptance Criteria

**Scenario 1** : Configure a website analytics
An authenticated user authorize `low-carbon-historian` to access his reporting API for a given website through OAuth 2.0, he then selects the website url to be analyzed. If he doesn't have Google Analytics enabled, a small survey is provided to get traffic source approximations.

**Scenario 2** : Export website analytics
A user downloads website analytics as a csv file 

**Scenario 3** : Visualize website analytics
A user see dashboard website analytics. He should see the following indicators :

for one day / for one site
- Sustainability Grade:A+
- Fact: Loading this pages takes the same amount of energy as 3.5 seconds in a microwave.
- kWh Emitted: 0.00035
- CO2 Emissions: 0.2g

## Stretch Goals

- How can we get website meta for home page
  
## Notes
To compute GPI's : 

- we need to crawl full website to get download size details on pages
- we need to get google analytics traffic for those pages (traffic by page and by country)
- when we have traffic by page we can compute GPI by page and for the whole site 

This app 

- Allows to visualize the electricity consumption and greenhouse gases (GHG) emissions that your website browsing leads to.
- Visualizing it will get you to understand that impacts of digital technologies on climate change and natural resources are not virtual, although they are hidden behind our screens.

To evaluate these impacts, the app:

- Measures the quantity of data travelling through an Internet browser,
- Calculates the electricity consumption this traffic leads to (with the "1byte" model, developed by The Shift Project),
- Calculates the GHG emissions this electricity consumption leads to, following the selected location.
  
##Resources

- https://github.com/carbonalyser/Carbonalyser
- https://developers.google.com/analytics/devguides/reporting/core/v4/authorization
- https://docs.easydigitaldownloads.com/article/1072-software-licensing-api
- https://theshiftproject.org/wp-content/uploads/2019/10/Lean-ICT-Materials-Liens-%C3%A0-t%C3%A9l%C3%A9charger-r%C3%A9par%C3%A9-le-29-10-2019.pdf
- 
