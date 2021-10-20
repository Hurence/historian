# Test project

Unit test should be implemented inside src/test directory and suffixed with "Test".
Integration tests should be put into src/integration-test and be suffixed with "IT" so that it is not run as Unit test !

To run integration tests

    mvn install -Pintegration-tests

with your IDE

mark the folder ./src/integration-test/java as source test code in your IDE.
mark the folder ./src/integration-test/resources as resources test in your IDE.

Then run :

    mvn clean install -Pbuild-integration-tests