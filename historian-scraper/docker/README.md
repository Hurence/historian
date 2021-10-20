# Docker

## Build
Use the following commands to build `hurence/historian-scrapper` container.

```bash
mvn clean install
docker build --rm -t hurence/historian-scrapper .
docker tag hurence/historian-scrapper:latest hurence/historian-scrapper:x.x.x
docker push  hurence/historian-scrapper:x.x.x
```

## Run
run a scrapper on a prometheus url

```bash
docker run -p 8081:8081 --env SCRAPER_URL=http://plouk:8080  hurence/historian-scrapper
```