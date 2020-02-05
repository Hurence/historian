


Data Historian Batch loading tool




# PRe load

    mvn clean install
    
    ../../spark-2.3.4-bin-hadoop2.7/bin/spark-submit --driver-memory 8g target/loader-1.3.0.jar com.hurence.historian.App --in ../../data/in/ISNTS35-N-2019-06 --out ../../data/out --mode preload --master local[*]
    
    
   ../../spark-2.3.4-bin-hadoop2.7/bin/spark-submit --jars bin/spark-sql-kafka-0-10_2.11-2.3.2.3.1.4.0-315.jar --driver-memory 8g target/loader-1.3.0.jar --mode chunk_by_file --master local[*] --brokers localhost:9092  --out evoa_chunks  --in ../../data/out/year=2019/*/*/*/*/*
   
   
   ../../spark-2.3.4-bin-hadoop2.7/bin/spark-submit --jars bin/spark-sql-kafka-0-10_2.11-2.3.2.3.1.4.0-315.jar --driver-memory 8g target/loader-1.3.0.jar --mode tag_chunk --master local[*] --out nothing --in ../../data/out/year=2019/*/*/*/*/*