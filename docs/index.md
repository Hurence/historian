---
layout: home
title: Welcome
---

Hurence has developed an open source Smart (Data) Historian. 
<p class="text-info">It is Big Data, AI Powered and can archive your time series (sensor data) without volume limitation with the performance of a simple to operate Big Data infrastructure and features worthy of historical data historians, and much more ...</p>





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



