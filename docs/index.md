---
layout: home
title: Welcome
---

Here is an open source Smart (Data) Historian. One-Stop shop for AI-Powered industrial data management system.

<p class="text-info">It is Big Data, AI Powered and can archive your time series (sensor data) without volume limitation with the performance of a simple to operate Big Data infrastructure and features worthy of historical data historians, and much more ...</p>

The Data historian has evolved from being just a place for storing data to becoming a data infrastructure. **Industry 4.0** evolution needs more with a complete infrastructure solution with the capability of integration, archiving, asset modeling, notifications, visualizing, analysis, and many more analyzing features. 




{% for item in site.data.pages.toc %}
<h3>{{ item.title }}</h3>
  <ul>
    {% for entry in item.subfolderitems %}

    <li class="{% if entry.url == page.url %}active{% endif %}">
      <span><a href="{{ entry.url }}"><b>{{ entry.page }}</b></a> : {{entry.content}}</span>
    </li>
    {% endfor %}
  </ul>
{% endfor %}



