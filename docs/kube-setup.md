---
layout: page
title: Kubernetes setup with Helm
---

This Helm chart bootstraps a deployment on a Kubernetes cluster using the package manager.

### Prerequisites

- Kubernetes 1.12+
- Helm 3.1.0
- PV provisioner support in the underlying infrastructure
- ReadWriteMany volumes for deployment scaling

### Installing the Chart
To install the chart with the release name `historian-demo` :

```bash
git clone https://github.com/Hurence/historian-helm.git
cd historian-helm
helm install historian-demo .
```
These commands deploy Historian stack on the Kubernetes cluster in the default configuration. The [parameters](#parameters) section lists the parameters that can be configured during installation.

> Tip: List all releases using helm list

### Remove your helm installation 
Be aware that all [Persistent Volume and Claims](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) will remain bound if you don't remove them explicitly. That's can be handy if you simply want to stop all your workloads without trashing all the data to save some resources while don't using them.

```bash
helm uninstall historian-demo
```


### Parameters



#### Historian parameters

| Name                             | Description                                     | Value   |
| -------------------------------- | ----------------------------------------------- | ------- |
| `global.imageRegistry`           | Global Docker image registry                    | `""`    |
| `global.imagePullSecrets`        | Global Docker registry secret names as an array | `[]`    |
| `global.storageClass`            | Global StorageClass for Persistent Volume(s)    | `""`    |
| `solr.replicaCount`              | Number of historian server replicas             | `1`     |
| `compactor.enabled`              | Do we compact chunks ?                          | `true`  |
| `scraper.enabled`                | Do we scrape prometheus metrics from exporter ? | `true`  |
| `scraper.batchSize`              | Size of solr chunk to be sent                   | `500`   |
| `scraper.delay`                  | Scrapping delay interval in ms                  | `10000` |


#### Solr parameters

The following parameters are inherited from [bitnami solr](https://github.com/bitnami/charts/tree/master/bitnami/solr) helm chart. So you can override these parameters directly in the `value.yaml` file by prefixing them with `solr`

```yaml
solr.authentication.enabled: no
```

Here is the list of all SolR common parameters


| Name                           | Description                                                   | Value                 |
| ------------------------------ | ------------------------------------------------------------- | --------------------- |
| `solr.image.registry`               | Solr image registry                                           | `docker.io`           |
| `solr.image.repository`             | Solr image repository                                         | `bitnami/solr`        |
| `solr.image.tag`                    | Solr image tag (immutable tags are recommended)               | `8.10.1-debian-10-r0` |
| `solr.image.pullPolicy`             | image pull policy                                             | `IfNotPresent`        |
| `solr.image.pullSecrets`            | Specify docker-registry secret names as an array              | `[]`                  |
| `solr.coreName`                     | Solr core name to be created                                  | `my-core`             |
| `solr.cloudEnabled`                 | Enable Solr cloud mode                                        | `true`                |
| `solr.cloudBootstrap`               | Enable cloud bootstrap. It will be performed from the node 0. | `true`                |
| `solr.collection`                   | Solr collection name                                          | `my-collection`       |
| `solr.collectionShards`             | Number of collection shards                                   | `1`                   |
| `solr.collectionReplicas`           | Number of collection replicas                                 | `2`                   |
| `solr.containerPort`                | Port number where Solr is running inside the container        | `8983`                |
| `solr.serverDirectory`              | Name of the created directory for the server                  | `server`              |
| `solr.javaMem`                      | Java memory options to pass to the Solr container             | `""`                  |
| `solr.heap`                         | Java Heap options to pass to the solr container               | `""`                  |
| `solr.authentication.enabled`       | Enable Solr authentication                                    | `true`                |
| `solr.authentication.adminUsername` | Solr admin username                                           | `admin`               |
| `solr.authentication.adminPassword` | Solr admin password. Autogenerated if not provided.           | `""`                  |
| `solr.existingSecret`               | Existing secret with Solr password                            | `""`                  |
| `solr.command`                      | Override Solr entrypoint string                               | `[]`                  |
| `solr.args`                         | Arguments for the provided command if needed                  | `[]`                  |
| `solr.extraEnvVars`                 | Additional environment variables to set                       | `[]`                  |
| `solr.extraEnvVarsCM`               | ConfigMap with extra environment variables                    | `""`                  |
| `solr.extraEnvVarsSecret`           | Secret with extra environment variables                       | `""`                  |


#### Solr statefulset parameters

| Name                                                      | Description                                                                                                                                         | Value                   |
| --------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------- |
| `solr.hostAliases`                                             | Deployment pod host aliases                                                                                                                         | `[]`                    |
| `solr.replicaCount`                                            | Number of solr replicas                                                                                                                             | `3`                     |
| `solr.livenessProbe.enabled`                                   | Enable livenessProbe                                                                                                                                | `true`                  |
| `solr.livenessProbe.initialDelaySeconds`                       | Initial delay seconds for livenessProbe                                                                                                             | `30`                    |
| `solr.livenessProbe.periodSeconds`                             | Period seconds for livenessProbe                                                                                                                    | `10`                    |
| `solr.livenessProbe.timeoutSeconds`                            | Timeout seconds for livenessProbe                                                                                                                   | `5`                     |
| `solr.livenessProbe.failureThreshold`                          | Failure threshold for livenessProbe                                                                                                                 | `6`                     |
| `solr.livenessProbe.successThreshold`                          | Success threshold for livenessProbe                                                                                                                 | `1`                     |
| `solr.readinessProbe.enabled`                                  | Enable readinessProbe                                                                                                                               | `true`                  |
| `solr.readinessProbe.initialDelaySeconds`                      | Initial delay seconds for readinessProbe                                                                                                            | `5`                     |
| `solr.readinessProbe.periodSeconds`                            | Period seconds for readinessProbe                                                                                                                   | `10`                    |
| `solr.readinessProbe.timeoutSeconds`                           | Timeout seconds for readinessProbe                                                                                                                  | `5`                     |
| `solr.readinessProbe.failureThreshold`                         | Failure threshold for readinessProbe                                                                                                                | `6`                     |
| `solr.readinessProbe.successThreshold`                         | Success threshold for readinessProbe                                                                                                                | `1`                     |
| `solr.resources.limits`                                        | The resources limits for the container                                                                                                              | `{}`                    |
| `solr.resources.requests`                                      | The requested resources for the container                                                                                                           | `{}`                    |
| `solr.containerSecurityContext.enabled`                        | Enable Solr containers' Security Context                                                                                                            | `true`                  |
| `solr.containerSecurityContext.runAsUser`                      | User ID for the containers                                                                                                                          | `1001`                  |
| `solr.containerSecurityContext.runAsNonRoot`                   | Enable Solr containers' Security Context runAsNonRoot                                                                                               | `true`                  |
| `solr.podSecurityContext.enabled`                              | Enable Solr pods' Security Context                                                                                                                  | `true`                  |
| `solr.podSecurityContext.fsGroup`                              | Group ID for the pods.                                                                                                                              | `1001`                  |
| `solr.podAffinityPreset`                                       | Solr pod affinity preset. Ignored if `affinity` is set. Allowed values: `soft` or `hard`                                                            | `""`                    |
| `solr.podAntiAffinityPreset`                                   | Solr pod anti-affinity preset. Ignored if `affinity` is set. Allowed values: `soft` or `hard`                                                       | `soft`                  |
| `solr.nodeAffinityPreset.type`                                 | Solr node affinity preset type. Ignored if `affinity` is set. Allowed values: `soft` or `hard`                                                      | `""`                    |
| `solr.nodeAffinityPreset.key`                                  | Solr node label key to match Ignored if `affinity` is set.                                                                                          | `""`                    |
| `solr.nodeAffinityPreset.values`                               | Solr node label values to match. Ignored if `affinity` is set.                                                                                      | `[]`                    |
| `solr.affinity`                                                | Affinity settings for Solr pod assignment. Evaluated as a template                                                                                  | `{}`                    |
| `solr.nodeSelector`                                            | Node labels for Solr pods assignment. Evaluated as a template                                                                                       | `{}`                    |
| `solr.tolerations`                                             | Tolerations for Solr pods assignment. Evaluated as a template                                                                                       | `[]`                    |
| `solr.podLabels`                                               | Additional labels for pods pod                                                                                                                      | `{}`                    |
| `solr.podAnnotations`                                          | Additional annotations for pods                                                                                                                     | `{}`                    |
| `solr.podManagementPolicy`                                     | Management Policy for Solr StatefulSet                                                                                                              | `Parallel`              |
| `solr.priorityClassName`                                       | Solr pods' priority.                                                                                                                                | `""`                    |
| `solr.lifecycleHooks`                                          | lifecycleHooks for the Solr container to automate configuration before or after startup                                                             | `{}`                    |
| `solr.customLivenessProbe`                                     | Override default liveness probe                                                                                                                     | `{}`                    |
| `solr.customReadinessProbe`                                    | Override default readiness probe                                                                                                                    | `{}`                    |
| `solr.updateStrategy.type`                                     | Update strategy - only really applicable for deployments with RWO PVs attached                                                                      | `RollingUpdate`         |
| `solr.updateStrategy.rollingUpdate`                            | Deployment rolling update configuration parameters                                                                                                  | `{}`                    |
| `solr.extraVolumes`                                            | Extra volumes to add to the deployment                                                                                                              | `[]`                    |
| `solr.extraVolumeMounts`                                       | Extra volume mounts to add to the container                                                                                                         | `[]`                    |
| `solr.initContainers`                                          | Add init containers to the Solr pods                                                                                                                | `[]`                    |
| `solr.sidecars`                                                | Add sidecars to the Solr pods                                                                                                                       | `[]`                    |
| `solr.volumePermissions.enabled`                               | Enable init container that changes volume permissions in the registry (for cases where the default k8s `runAsUser` and `fsUser` values do not work) | `false`                 |
| `solr.volumePermissions.image.registry`                        | Init container volume-permissions image registry                                                                                                    | `docker.io`             |
| `solr.volumePermissions.image.repository`                      | Init container volume-permissions image name                                                                                                        | `bitnami/bitnami-shell` |
| `solr.volumePermissions.image.tag`                             | Init container volume-permissions image tag                                                                                                         | `10-debian-10-r224`     |
| `solr.volumePermissions.image.pullPolicy`                      | Init container volume-permissions image pull policy                                                                                                 | `IfNotPresent`          |
| `solr.volumePermissions.image.pullSecrets`                     | Specify docker-registry secret names as an array                                                                                                    | `[]`                    |
| `solr.volumePermissions .resources.limits`                      | The resources limits for the container                                                                                                              | `{}`                    |
| `solr.volumePermissions .resources.requests`                    | The requested resources for the container                                                                                                           | `{}`                    |
| `solr.volumePermissions .containerSecurityContext.enabled`      | Container security context for volume permissions                                                                                                   | `true`                  |
| `solr.volumePermissions .containerSecurityContext.runAsUser`    | Container security context fsGroup for volume permissions                                                                                           | `1001`                  |
| `solr.volumePermissions .containerSecurityContext.runAsNonRoot` | Container security context runAsNonRoot for volume permissions                                                                                      | `true`                  |
| `solr.persistence.enabled`                                     | Use a PVC to persist data.                                                                                                                          | `true`                  |
| `solr.persistence.existingClaim`                               | A manually managed Persistent Volume and Claim                                                                                                      | `""`                    |
| `solr.persistence.storageClass`                                | Storage class of backing PVC                                                                                                                        | `""`                    |
| `solr.persistence.accessModes`                                 | Persistent Volume Access Modes                                                                                                                      | `["ReadWriteOnce"]`     |
| `solr.persistence.size`                                        | Size of data volume                                                                                                                                 | `8Gi`                   |
| `solr.persistence.annotations`                                 | Persistence annotations for Solr                                                                                                                    | `{}`                    |
| `solr.persistence.mountPath`                                   | Persistence mount path for Solr                                                                                                                     | `/bitnami/solr`         |
| `solr.persistence.extraVolumeClaimTemplates`                   | Additional pod instance specific volumes                                                                                                            | `[]`                    |
| `solr.serviceAccount.create`                                   | Specifies whether a ServiceAccount should be created                                                                                                | `false`                 |
| `solr.serviceAccount.name`                                     | The name of the ServiceAccount to create                                                                                                            | `""`                    |


#### Solr SSL parameters

| Name                         | Description                                                                      | Value   |
| ---------------------------- | -------------------------------------------------------------------------------- | ------- |
| `solr.tls.enabled`                | Enable the TLS/SSL configuration                                                 | `false` |
| `solr.tls.autoGenerated`          | Create self-signed TLS certificates. Currently only supports PEM certificates    | `false` |
| `solr.tls.certificatesSecretName` | Name of the secret that contains the certificates                                | `""`    |
| `solr.tls.passwordsSecretName`    | Set the name of the secret that contains the passwords for the certificate files | `""`    |
| `solr.tls.keystorePassword`       | Password to access the keystore when it's password-protected                     | `""`    |
| `solr.tls.truststorePassword`     | Password to access the truststore when it's password-protected                   | `""`    |
| `solr.tls.resources.limits`       | The resources limits for the TLS init container                                  | `{}`    |
| `solr.tls.resources.requests`     | The requested resources for the TLS init container                               | `{}`    |


#### Solr Traffic Exposure Parameters

| Name                      | Description                                                                                                                      | Value                    |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ------------------------ |
| `solr.service.type`            | Service type for default solr service                                                                                            | `ClusterIP`              |
| `solr.service.port`            | HTTP Port                                                                                                                        | `8983`                   |
| `solr.service.annotations`     | Annotations for solr service                                                                                                     | `{}`                     |
| `solr.service.labels`          | Additional labels for solr service                                                                                               | `{}`                     |
| `solr.service.loadBalancerIP`  | Load balancer IP for the Solr Service (optional, cloud specific)                                                                 | `""`                     |
| `solr.service.nodePorts.http`  | Node ports for the HTTP service                                                                                                  | `""`                     |
| `solr.service.nodePorts.https` | Node ports for the HTTPS service                                                                                                 | `""`                     |
| `solr.ingress.enabled`         | Enable ingress controller resource                                                                                               | `false`                  |
| `solr.ingress.pathType`        | Path type for the ingress resource                                                                                               | `ImplementationSpecific` |
| `solr.ingress.apiVersion`      | Override API Version (automatically detected if not set)                                                                         | `""`                     |
| `solr.ingress.hostname`        | Default host for the ingress resource                                                                                            | `solr.local`             |
| `solr.ingress.path`            | The Path to Solr. You may need to set this to '/*' in order to use this with ALB ingress controllers.                            | `/`                      |
| `solr.ingress.annotations`     | Additional annotations for the Ingress resource. To enable certificate autogeneration, place here your cert-manager annotations. | `{}`                     |
| `solr.ingress.tls`             | Enable TLS configuration for the hostname defined at ingress.hostname parameter                                                  | `false`                  |
| `solr.ingress.existingSecret`  | The name of an existing Secret in the same namespase to use on the generated Ingress resource                                    | `""`                     |
| `solr.ingress.extraHosts`      | The list of additional hostnames to be covered with this ingress record.                                                         | `[]`                     |
| `solr.ingress.extraPaths`      | Any additional arbitrary paths that may need to be added to the ingress under the main host.                                     | `[]`                     |
| `solr.ingress.extraTls`        | The tls configuration for additional hostnames to be covered with this ingress record.                                           | `[]`                     |
| `solr.ingress.secrets`         | If you're providing your own certificates, please use this to add the certificates as secrets                                    | `[]`                     |


#### Zookeeper parameters

| Name                                         | Description                                         | Value                 |
| -------------------------------------------- | --------------------------------------------------- | --------------------- |
| `zookeeper.enabled`                          | Enable Zookeeper deployment. Needed for Solr cloud  | `true`                |
| `zookeeper.persistence.enabled`              | Enabled persistence for Zookeeper                   | `true`                |
| `zookeeper.port`                             | Zookeeper port service port                         | `2181`                |
| `zookeeper.replicaCount`                     | Number of Zookeeper cluster replicas                | `3`                   |
| `zookeeper.fourlwCommandsWhitelist`          | Zookeeper four letters commands to enable           | `srvr,mntr,conf,ruok` |
| `zookeeper.service.publishNotReadyAddresses` | Publish not Ready ips for zookeeper                 | `true`                |
| `zookeeper.externalZookeeper.servers`                  | Server or list of external zookeeper servers to use | `[]`                  |


#### Solr Exporter deployment parameters

| Name                                             | Description                                                                                                                     | Value                                                                         |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `solr.exporter.enabled`                               | Start a side-car prometheus exporter                                                                                            | `false`                                                                       |
| `solr.exporter.livenessProbe.enabled`                 | Enable livenessProbe                                                                                                            | `true`                                                                        |
| `solr.exporter.livenessProbe.initialDelaySeconds`     | Initial delay seconds for livenessProbe                                                                                         | `10`                                                                          |
| `solr.exporter.livenessProbe.periodSeconds`           | Period seconds for livenessProbe                                                                                                | `5`                                                                           |
| `solr.exporter.livenessProbe.timeoutSeconds`          | Timeout seconds for livenessProbe                                                                                               | `15`                                                                          |
| `solr.exporter.livenessProbe.failureThreshold`        | Failure threshold for livenessProbe                                                                                             | `15`                                                                          |
| `solr.exporter.livenessProbe.successThreshold`        | Success threshold for livenessProbe                                                                                             | `1`                                                                           |
| `solr.exporter.readinessProbe.enabled`                | Enable readinessProbe                                                                                                           | `true`                                                                        |
| `solr.exporter.readinessProbe.initialDelaySeconds`    | Initial delay seconds for readinessProbe                                                                                        | `10`                                                                          |
| `solr.exporter.readinessProbe.periodSeconds`          | Period seconds for readinessProbe                                                                                               | `5`                                                                           |
| `solr.exporter.readinessProbe.timeoutSeconds`         | Timeout seconds for readinessProbe                                                                                              | `15`                                                                          |
| `solr.exporter.readinessProbe.failureThreshold`       | Failure threshold for readinessProbe                                                                                            | `15`                                                                          |
| `solr.exporter.readinessProbe.successThreshold`       | Success threshold for readinessProbe                                                                                            | `15`                                                                          |
| `solr.exporter.configFile`                            | Config file with metrics to export by the Solr prometheus exporter. To change it mount a different file using `extraConfigMaps` | `/opt/bitnami/solr/contrib/prometheus-exporter/conf/solr-exporter-config.xml` |
| `solr.exporter.port`                                  | Solr exporter port                                                                                                              | `9983`                                                                        |
| `solr.exporter.threads`                               | Number of Solr exporter threads                                                                                                 | `7`                                                                           |
| `solr.exporter.command`                               | Override Solr entrypoint string.                                                                                                | `[]`                                                                          |
| `solr.exporter.args`                                  | Arguments for the provided command if needed                                                                                    | `[]`                                                                          |
| `solr.exporter.resources.limits`                      | The resources limits for the container                                                                                          | `{}`                                                                          |
| `solr.exporter.resources.requests`                    | The requested resources for the container                                                                                       | `{}`                                                                          |
| `solr.exporter .containerSecurityContext.enabled`      | Enable Solr exporter containers' Security Context                                                                               | `true`                                                                        |
| `solr.exporter .containerSecurityContext.runAsUser`    | User ID for the containers.                                                                                                     | `1001`                                                                        |
| `solr.exporter .containerSecurityContext.runAsNonRoot` | Enable Solr exporter containers' Security Context runAsNonRoot                                                                  | `true`                                                                        |
| `solr.exporter.podSecurityContext.enabled`            | Enable Solr exporter pods' Security Context                                                                                     | `true`                                                                        |
| `solr.exporter.podSecurityContext.fsGroup`            | Group ID for the pods.                                                                                                          | `1001`                                                                        |
| `solr.exporter.podAffinityPreset`                     | Solr pod affinity preset. Ignored if `affinity` is set. Allowed values: `soft` or `hard`                                        | `""`                                                                          |
| `solr.exporter.podAntiAffinityPreset`                 | Solr pod anti-affinity preset. Ignored if `affinity` is set. Allowed values: `soft` or `hard`                                   | `soft`                                                                        |
| `solr.exporter.nodeAffinityPreset.type`               | Solr node affinity preset type. Ignored if `affinity` is set. Allowed values: `soft` or `hard`                                  | `""`                                                                          |
| `solr.exporter.nodeAffinityPreset.key`                | Solr node label key to match Ignored if `affinity` is set.                                                                      | `""`                                                                          |
| `solr.exporter.nodeAffinityPreset.values`             | Solr node label values to match. Ignored if `affinity` is set.                                                                  | `[]`                                                                          |
| `solr.exporter.affinity`                              | Affinity settings for Solr pod assignment. Evaluated as a template                                                              | `{}`                                                                          |
| `solr.exporter.nodeSelector`                          | Node labels for Solr pods assignment. Evaluated as a template                                                                   | `{}`                                                                          |
| `solr.exporter.tolerations`                           | Tolerations for Solr pods assignment. Evaluated as a template                                                                   | `[]`                                                                          |
| `solr.exporter.podLabels`                             | Additional labels for Metrics exporter pod                                                                                      | `{}`                                                                          |
| `solr.exporter.podAnnotations`                        | Additional annotations for Metrics exporter pod                                                                                 | `{}`                                                                          |
| `solr.exporter.customLivenessProbe`                   | Override default liveness probe%%MAIN_CONTAINER_NAME%%                                                                          | `{}`                                                                          |
| `solr.exporter.customReadinessProbe`                  | Override default readiness probe%%MAIN_CONTAINER_NAME%%                                                                         | `{}`                                                                          |
| `solr.exporter.updateStrategy.type`                   | Update strategy - only really applicable for deployments with RWO PVs attached                                                  | `RollingUpdate`                                                               |
| `solr.exporter.updateStrategy.rollingUpdate`          | Deployment rolling update configuration parameters                                                                              | `{}`                                                                          |
| `solr.exporter.extraEnvVars`                          | Additional environment variables to set                                                                                         | `[]`                                                                          |
| `solr.exporter.extraEnvVarsCM`                        | ConfigMap with extra environment variables                                                                                      | `""`                                                                          |
| `solr.exporter.extraEnvVarsSecret`                    | Secret with extra environment variables                                                                                         | `""`                                                                          |
| `solr.exporter.extraVolumes`                          | Extra volumes to add to the deployment                                                                                          | `[]`                                                                          |
| `solr.exporter.extraVolumeMounts`                     | Extra volume mounts to add to the container                                                                                     | `[]`                                                                          |
| `solr.exporter.initContainers`                        | Add init containers to the %%MAIN_CONTAINER_NAME%% pods                                                                         | `[]`                                                                          |
| `solr.exporter.sidecars`                              | Add sidecars to the %%MAIN_CONTAINER_NAME%% pods                                                                                | `[]`                                                                          |
| `solr.exporter.service.type`                          | Service type for default solr exporter service                                                                                  | `ClusterIP`                                                                   |
| `solr.exporter.service.annotations`                   | annotations for solr exporter service                                                                                           | `{}`                                                                          |
| `solr.exporter.service.labels`                        | Additional labels for solr exporter service                                                                                     | `{}`                                                                          |
| `solr.exporter.service.port`                          | Kubernetes Service port                                                                                                         | `9983`                                                                        |
| `solr.exporter.service.loadBalancerIP`                | Load balancer IP for the Solr Exporter Service (optional, cloud specific)                                                       | `""`                                                                          |
| `solr.exporter.service.nodePorts.http`                | Node ports for the HTTP exporter service                                                                                        | `""`                                                                          |
| `solr.exporter.service.nodePorts.https`               | Node ports for the HTTPS exporter service                                                                                       | `""`                                                                          |
| `solr.exporter.service.loadBalancerSourceRanges`      | Exporter Load Balancer Source ranges                                                                                            | `[]`                                                                          |
| `solr.exporter.hostAliases`                           | Deployment pod host aliases                                                                                                     | `[]`                                                                          |



## Grafana parameters

The following parameters are inherited from [bitnami grafan](https://github.com/bitnami/charts/tree/master/bitnami/grafana) helm chart. So you can override these parameters directly in the `value.yaml` file by prefixing them with `grafana`

```yaml
grafana.admin.password: historian
grafana.admin.existingSecret: historian
```


### Grafana parameters

| Name                               | Description                                                                       | Value                |
| ---------------------------------- | --------------------------------------------------------------------------------- | -------------------- |
| `grafana.image.registry`                   | Grafana image registry                                                            | `docker.io`          |
| `grafana.image.repository`                 | Grafana image repository                                                          | `bitnami/grafana`    |
| `grafana.image.tag`                        | Grafana image tag (immutable tags are recommended)                                | `8.2.2-debian-10-r0` |
| `grafana.image.pullPolicy`                 | Grafana image pull policy                                                         | `IfNotPresent`       |
| `grafana.image.pullSecrets`                | Grafana image pull secrets                                                        | `[]`                 |
| `grafana.hostAliases`                      | Add deployment host aliases                                                       | `[]`                 |
| `grafana.admin.user`                       | Grafana admin username                                                            | `admin`              |
| `grafana.admin.password`                   | Admin password. If a password is not provided a random password will be generated | `""`                 |
| `grafana.admin.existingSecret`             | Name of the existing secret containing admin password                             | `""`                 |
| `grafana.admin.existingSecretPasswordKey`  | Password key on the existing secret                                               | `password`           |
| `grafana.smtp.enabled`                     | Enable SMTP configuration                                                         | `false`              |
| `grafana.smtp.user`                        | SMTP user                                                                         | `user`               |
| `grafana.smtp.password`                    | SMTP password                                                                     | `password`           |
| `grafana.smtp.host`                        | Custom host for the smtp server                                                   | `""`                 |
| `grafana.smtp.existingSecret`              | Name of existing secret containing SMTP credentials (user and password)           | `""`                 |
| `grafana.smtp.existingSecretUserKey`       | User key on the existing secret                                                   | `user`               |
| `grafana.smtp.existingSecretPasswordKey`   | Password key on the existing secret                                               | `password`           |
| `grafana.plugins`                          | Grafana plugins to be installed in deployment time separated by commas            | `""`                 |
| `grafana.ldap.enabled`                     | Enable LDAP for Grafana                                                           | `false`              |
| `grafana.ldap.allowSignUp`                 | Allows LDAP sign up for Grafana                                                   | `false`              |
| `grafana.ldap.configuration`               | Specify content for ldap.toml configuration file                                  | `""`                 |
| `grafana.ldap.configMapName`               | Name of the ConfigMap with the ldap.toml configuration file for Grafana           | `""`                 |
| `grafana.ldap.secretName`                  | Name of the Secret with the ldap.toml configuration file for Grafana              | `""`                 |
| `grafana.config.useGrafanaIniFile`         | Allows to load a `grafana.ini` file                                               | `false`              |
| `grafana.config.grafanaIniConfigMap`       | Name of the ConfigMap containing the `grafana.ini` file                           | `""`                 |
| `grafana.config.grafanaIniSecret`          | Name of the Secret containing the `grafana.ini` file                              | `""`                 |
| `grafana.dashboardsProvider.enabled`       | Enable the use of a Grafana dashboard provider                                    | `false`              |
| `grafana.dashboardsProvider.configMapName` | Name of a ConfigMap containing a custom dashboard provider                        | `""`                 |
| `grafana.dashboardsConfigMaps`             | Array with the names of a series of ConfigMaps containing dashboards files        | `[]`                 |
| `grafana.datasources.secretName`           | Secret name containing custom datasource files                                    | `""`                 |


### Grafana Deployment parameters

| Name                                         | Description                                                                                             | Value           |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------- | --------------- |
| `grafana.replicaCount`                       | Number of Grafana nodes                                                                                 | `1`             |
| `grafana.updateStrategy.type`                | Set up update strategy for Grafana installation.                                                        | `RollingUpdate` |
| `grafana.schedulerName`                      | Alternative scheduler                                                                                   | `""`            |
| `grafana.priorityClassName`                  | Priority class name                                                                                     | `""`            |
| `grafana.podLabels`                          | Extra labels for Grafana pods                                                                           | `{}`            |
| `grafana.podAnnotations`                     | Grafana Pod annotations                                                                                 | `{}`            |
| `grafana.podAffinityPreset`                  | Pod affinity preset. Ignored if `affinity` is set. Allowed values: `soft` or `hard`                     | `""`            |
| `grafana.podAntiAffinityPreset`              | Pod anti-affinity preset. Ignored if `affinity` is set. Allowed values: `soft` or `hard`                | `soft`          |
| `grafana.containerPorts.grafana`             | Grafana container port                                                                                  | `3000`          |
| `grafana.nodeAffinityPreset.type`            | Node affinity preset type. Ignored if `affinity` is set. Allowed values: `soft` or `hard`               | `""`            |
| `grafana.nodeAffinityPreset.key`             | Node label key to match Ignored if `affinity` is set.                                                   | `""`            |
| `grafana.nodeAffinityPreset.values`          | Node label values to match. Ignored if `affinity` is set.                                               | `[]`            |
| `grafana.affinity`                           | Affinity for pod assignment                                                                             | `{}`            |
| `grafana.nodeSelector`                       | Node labels for pod assignment                                                                          | `{}`            |
| `grafana.tolerations`                        | Tolerations for pod assignment                                                                          | `[]`            |
| `grafana.topologySpreadConstraints`          | Topology spread constraints rely on node labels to identify the topology domain(s) that each Node is in | `[]`            |
| `grafana.podSecurityContext.enabled`         | Enable securityContext on for Grafana deployment                                                        | `true`          |
| `grafana.podSecurityContext.fsGroup`         | Group to configure permissions for volumes                                                              | `1001`          |
| `grafana.podSecurityContext.runAsUser`       | User for the security context                                                                           | `1001`          |
| `grafana.podSecurityContext.runAsNonRoot`    | Run containers as non-root users                                                                        | `true`          |
| `grafana.containerSecurityContext.enabled`   | Enabled Grafana Image Renderer containers' Security Context                                             | `true`          |
| `grafana.containerSecurityContext.runAsUser` | Set Grafana Image Renderer containers' Security Context runAsUser                                       | `1001`          |
| `grafana.resources.limits`                   | The resources limits for Grafana containers                                                             | `{}`            |
| `grafana.resources.requests`                 | The requested resources for Grafana containers                                                          | `{}`            |
| `grafana.livenessProbe.enabled`              | Enable livenessProbe                                                                                    | `true`          |
| `grafana.livenessProbe.path`                 | Path for livenessProbe                                                                                  | `/api/health`   |
| `grafana.livenessProbe.scheme`               | Scheme for livenessProbe                                                                                | `HTTP`          |
| `grafana.livenessProbe.initialDelaySeconds`  | Initial delay seconds for livenessProbe                                                                 | `120`           |
| `grafana.livenessProbe.periodSeconds`        | Period seconds for livenessProbe                                                                        | `10`            |
| `grafana.livenessProbe.timeoutSeconds`       | Timeout seconds for livenessProbe                                                                       | `5`             |
| `grafana.livenessProbe.failureThreshold`     | Failure threshold for livenessProbe                                                                     | `6`             |
| `grafana.livenessProbe.successThreshold`     | Success threshold for livenessProbe                                                                     | `1`             |
| `grafana.readinessProbe.enabled`             | Enable readinessProbe                                                                                   | `true`          |
| `grafana.readinessProbe.path`                | Path for readinessProbe                                                                                 | `/api/health`   |
| `grafana.readinessProbe.scheme`              | Scheme for readinessProbe                                                                               | `HTTP`          |
| `grafana.readinessProbe.initialDelaySeconds` | Initial delay seconds for readinessProbe                                                                | `30`            |
| `grafana.readinessProbe.periodSeconds`       | Period seconds for readinessProbe                                                                       | `10`            |
| `grafana.readinessProbe.timeoutSeconds`      | Timeout seconds for readinessProbe                                                                      | `5`             |
| `grafana.readinessProbe.failureThreshold`    | Failure threshold for readinessProbe                                                                    | `6`             |
| `grafana.readinessProbe.successThreshold`    | Success threshold for readinessProbe                                                                    | `1`             |
| `grafana.startupProbe.enabled`               | Enable startupProbe                                                                                     | `false`         |
| `grafana.startupProbe.path`                  | Path for readinessProbe                                                                                 | `/api/health`   |
| `grafana.startupProbe.scheme`                | Scheme for readinessProbe                                                                               | `HTTP`          |
| `grafana.startupProbe.initialDelaySeconds`   | Initial delay seconds for startupProbe                                                                  | `30`            |
| `grafana.startupProbe.periodSeconds`         | Period seconds for startupProbe                                                                         | `10`            |
| `grafana.startupProbe.timeoutSeconds`        | Timeout seconds for startupProbe                                                                        | `5`             |
| `grafana.startupProbe.failureThreshold`      | Failure threshold for startupProbe                                                                      | `6`             |
| `grafana.startupProbe.successThreshold`      | Success threshold for startupProbe                                                                      | `1`             |
| `grafana.customLivenessProbe`                | Custom livenessProbe that overrides the default one                                                     | `{}`            |
| `grafana.customReadinessProbe`               | Custom readinessProbe that overrides the default one                                                    | `{}`            |
| `grafana.customStartupProbe`                 | Custom startupProbe that overrides the default one                                                      | `{}`            |
| `grafana.lifecycleHooks`                     | for the Grafana container(s) to automate configuration before or after startup                          | `{}`            |
| `grafana.sidecars`                           | Attach additional sidecar containers to the Grafana pod                                                 | `[]`            |
| `grafana.initContainers`                     | Add additional init containers to the Grafana pod(s)                                                    | `{}`            |
| `grafana.extraVolumes`                       | Additional volumes for the Grafana pod                                                                  | `[]`            |
| `grafana.extraVolumeMounts`                  | Additional volume mounts for the Grafana container                                                      | `[]`            |
| `grafana.extraEnvVarsCM`                     | Name of existing ConfigMap containing extra env vars for %%MAIN_CONTAINER_NAME%% nodes                  | `""`            |
| `grafana.extraEnvVarsSecret`                 | Name of existing Secret containing extra env vars for %%MAIN_CONTAINER_NAME%% nodes                     | `""`            |
| `grafana.extraEnvVars`                       | Array containing extra env vars to configure Grafana                                                    | `{}`            |
| `grafana.extraConfigmaps`                    | Array to mount extra ConfigMaps to configure Grafana                                                    | `{}`            |
| `grafana.command`                            | Override default container command (useful when using custom images)                                    | `[]`            |
| `grafana.args`                               | Override default container args (useful when using custom images)                                       | `[]`            |


### Grafana Persistence parameters

| Name                        | Description                                                                                               | Value           |
| --------------------------- | --------------------------------------------------------------------------------------------------------- | --------------- |
| `grafana.persistence.enabled`       | Enable persistence                                                                                        | `true`          |
| `grafana.persistence.annotations`   | Persistent Volume Claim annotations                                                                       | `{}`            |
| `grafana.persistence.accessMode`    | Persistent Volume Access Mode                                                                             | `ReadWriteOnce` |
| `grafana.persistence.accessModes`   | Persistent Volume Access Modes                                                                            | `[]`            |
| `grafana.persistence.storageClass`  | Storage class to use with the PVC                                                                         | `""`            |
| `grafana.persistence.existingClaim` | If you want to reuse an existing claim, you can pass the name of the PVC using the existingClaim variable | `""`            |
| `grafana.persistence.size`          | Size for the PV                                                                                           | `10Gi`          |



Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example,

```console
$ helm install my-release \
  --set cloudEnabled=true bitnami/solr
```