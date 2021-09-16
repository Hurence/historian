# Getting Started



## Docker 

```bash
mvn clean install
docker build --rm -t hurence/historian-scrapper .
docker run -p 8081:8081 --env SCRAPPER_URL=http://plouk:8080  hurence/historian-scrapper
docker tag hurence/historian-scrapper:latest hurence/historian-scrapper:1.3.8
docker push  hurence/historian-scrapper:1.3.8
```