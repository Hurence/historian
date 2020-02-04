#!/usr/bin/env bash


../../spark-2.3.4-bin-hadoop2.7/bin/spark-submit  --jars bin/spark-sql-kafka-0-10_2.11/2.3.2.3.1.4.0-315/spark-sql-kafka-0-10_2.11-2.3.2.3.1.4.0-315.jar  --driver-memory 8g target/loader-1.3.0.jar --in ../../ifpen/data-historian/data/preload --out evoa_chunks --master local[*] --mode chunk --brokers localhost:9092l
