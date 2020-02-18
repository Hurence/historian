#Add a collection for integration test

## How

If you need a new solr collection for your test, here are the steps.

Copy paste one of the existing collection conf , for exemple :

```
cp ./integration-test/solr/configsets/historian ./integration-test/solr/configsets/<my-new-collection>
```

Then just change the file "managed-schema" of the directory to add your fields instead of the previous collection.

Go to the SolrExtension class at line 112, and add your conf template to zookeeper.

Then do a request somewhere to create the collection with the added template, there is some example in HistorianSolrITHelper.

## Debug

Run a test to verify that the index is correctly created and that you can inject your docs.

If there is a problem run the test in debug mode and insert a breakpoint so that the test hang on.
This way we will be able to check at container logs that are launched for the test.

```shell script
docker ps   
```

should return something like 

```shell script
CONTAINER ID        IMAGE                               COMMAND                  CREATED             STATUS              PORTS                                              NAMES
e8f1f1d1f8b3        alpine/socat:latest                 "/bin/sh -c 'socat T…"   20 minutes ago      Up 20 minutes       0.0.0.0:32776->2000/tcp, 0.0.0.0:32775->2001/tcp   testcontainers-socat-xc9UbPiZ
e52fe1c3af2e        solr:8.2.0                          "docker-entrypoint.s…"   20 minutes ago      Up 20 minutes       8983/tcp                                           x8j3c5bytsj2_solr2_1
e07a920fea46        solr:8.2.0                          "docker-entrypoint.s…"   20 minutes ago      Up 20 minutes       8983/tcp                                           x8j3c5bytsj2_solr1_1
9c65b06fcd37        wurstmeister/zookeeper              "/bin/sh -c '/usr/sb…"   20 minutes ago      Up 20 minutes       22/tcp, 2181/tcp, 2888/tcp, 3888/tcp               x8j3c5bytsj2_zookeeper_1
3d90c6ab6046        quay.io/testcontainers/ryuk:0.2.3   "/app"                   20 minutes ago      Up 20 minutes       0.0.0.0:32774->8080/tcp                            testcontainers-ryuk-423c0d7b-7fb
```

For creating a collection we will check logs in solr1, solr2 and zookeeper. At first solr1

```shell script
docker logs -f x8j3c5bytsj2_solr1_1
```

Check for any error and reason while the collection creation or injection failed.