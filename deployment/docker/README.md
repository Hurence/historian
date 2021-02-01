# Hurence Historian on Docker

To learn more on how to run Hurence Historian using Docker, please consult [developer manual](https://github.com/hurence/historian/docs).

The most recent version of the documentation can be viewed online at:


## How to run Hurence Historian on Docker.

You are required to have Docker installed and running.<br>
It is advisable to give to Docker at least 6GB of ram and 2 CPUs. 

### Running
Just simply run:
```bash
./docker-deploy.sh
```

PowerShell scripts are also available for Windows systems:
```bash
.\docker-deploy.ps1
```

### Accessing components
After deployment and startup of containers, they can be accessed at the following endpoints.


| Application/Service | Endpoint       | User         | Password       | Others                            |
| ------------------- | -------------- | ------------ | -------------- | --------------------------------- |
| Zookeeper           | localhost:9983 |              |                |                                   |
| SolR Core 1         | localhost:8983 |              |                |                                   |
| SolR Core 2         | localhost:8983 |              |                |                                   |
| Grafana             | localhost:3080 |              |                |                                   |
| REST API endpoint   | localhost:8081 |              |                |                                   |

### Checking
You can check Docker containers logs to check that everything has started and is running properly, by running:
```bash
./docker-logs.sh
```

### Tear down
To stop and remove all containers, simply run:
```bash
./docker-undeploy.sh
```


### Advanced options

#### Setting the Hurence Historian version
Other than the default deployment it is possible to run other versions of Hurence Historian.

By default the `latest` version of Hurence Historian will be brought up. 
You can change the version of Hurence Historian by exporting the environment variable `IMAGE_VERSION`. 
Please also remember to checkout the related git tag first.

Example:
```bash
git checkout 1.0.0-M5
export IMAGE_VERSION=1.0.0-M5
./docker-deploy.sh
```

#### Building containers from scratch
If you want to build containers from the code, you'll need to build the whole Hurence Historian Project.

From the project root directory, run:
```bash
mvn clean install -Pdocker
```

To build also the Admin Web Console container, which is excluded by default, add the `console` profile:
```bash
mvn clean install -Pconsole,docker
```

After the build has completed follow the steps from the [Running](#Running) section.

