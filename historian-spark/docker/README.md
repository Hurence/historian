Historian docker files
======================

Build your own
--------------

use appropriate version of jar !

Build historian-server

```shell script
mvn clean package
```

copy paste jar and conf file

```shell script
cd ./docker
cp ../../assembly/target/historian-1.3.8-bin.tgz .
```  
  
Building the image, modify version of jar in ENTRYPOINT if needed

```shell script
docker build --rm -t hurence/historian-spark .
docker tag hurence/historian-spark:latest hurence/historian-spark:1.3.8
```

Deploy the image to Docker hub
------------------------------

tag the image as latest

verify image build :

```shell script
docker images
docker tag <IMAGE_ID> latest
```

then login and push the latest image

```shell script
docker login
docker push hurence/historian-spark
docker push hurence/historian-spark:1.3.8
````


