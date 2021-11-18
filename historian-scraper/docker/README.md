# Docker

## Build
Use the following commands to build `hurence/historian-scraper` container.

```bash
mvn clean install

cd docker
cp ../target/historian-scrapper-0.0.1-SNAPSHOT.jar .


docker build --rm -t hurence/historian-scraper .
docker tag hurence/historian-scraper:latest hurence/historian-scraper:1.3.9
docker push  hurence/historian-scraper:1.3.9
docker push  hurence/historian-scraper
```

## Run
run a scrapper on a prometheus url

```bash
docker run -p 8081:8081 --env SCRAPER_URL=http://plouk:8080  hurence/historian-scraper
```