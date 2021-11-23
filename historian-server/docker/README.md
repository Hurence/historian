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
cp ../target/historian-server-1.3.9-fat.jar .
cp ../target/classes/config.json .

cd  ../../historian-resources/conf/solr/conf/
zip -r historian-configset.zip ./*
cd -;
mv ../../historian-resources/conf/solr/conf/historian-configset.zip .

```  
  
Building the image, modify version of jar in ENTRYPOINT if needed

```shell script
docker build --rm -t hurence/historian-server .
docker tag hurence/historian-server:latest hurence/historian-server:1.3.9
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
docker push hurence/historian-server
docker push hurence/historian-server:1.3.9
````


