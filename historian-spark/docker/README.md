Historian docker files
======================

Build your own
--------------

use appropriate version of jar !

Build historian-server


> Make sure you have updated correctly the release version number (especially in `historian-resources/bin/common.sh`) scirpts
```shell script
mvn clean package
```

copy paste jar and conf file

```shell script
cd ./docker
cp ../../assembly/target/historian-*-bin.tgz .
```  
  
Building the image, modify version of jar in ENTRYPOINT if needed

```shell script
docker build --rm -t hurence/historian-spark .
docker tag hurence/historian-spark:latest hurence/historian-spark:1.3.9
```

Deploy the image to Docker hub
------------------------------

then login and push the latest image

```shell script
docker login
docker push hurence/historian-spark
docker push hurence/historian-spark:1.3.9
````


