# Stargate Software Development Kit

[![License Apache2](https://img.shields.io/hexpm/l/plug.svg)](http://www.apache.org/licenses/LICENSE-2.0)

## Table of Contents

[**1. Stargate Apis Overview**](#1-database-initialization)
- [1.1 - Create Astra Account](#11---create-an-astra-account)

[**2. Stargate SDK**](#1-database-initialization)
- [2.1. Pre-requisites](#pre-requisites)
- [2.2 - QuickStart](#quickstart)
- [2.3 - Working with Rest API](#quickstart)
- [2.4 - Working with Document API](#quickstart)

## 1. Stargate Apis Overview

[Stargate](stargate.io) is a data API gateway that deploys between your apps and your Apache Cassandra database(s). Stargate is a framework used to customize all aspects of data access. It is deployed between client applications and a database to provide an abstraction layer that can be used to shape your data access to fit your application’s needs.

Stargate exposes multiple APis to access data stored in Cassandra, including REST, GraphQL, and schemaless Document APIs. This SDK provides a Java API to access to all of them.


| API                                                                               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|-----------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [CQL](https://stargate.io/docs/latest/develop/dev-with-cql.html)                  | Stargate functions as a Cassandra node, allowing existing Cassandra drivers to be utilized for connections. The primary goal is to minimize the number of open connections by enabling clients to connect only to Stargate, rather than directly to the Cassandra data nodes. <p>This approach also serves as an effective method to separate computing and storage components.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| [JSON API](https://github.com/stargate/jsonapi/blob/main/docs/jsonapi-spec.md)   | Mongoose compatible Http API exposing operation to use Cassandra as a document database. It should be consider and an upgrade to the previously discussed document API. It also introduces support for vectors and semantic searches.
| [REST](https://stargate.io/docs/latest/develop/dev-with-rest.html)                | Stargate is a data gateway deployed between client applications and a database. The REST API exposes CRUD access to data stored in Cassandra tables.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| [Document](https://stargate.io/docs/latest/develop/dev-with-doc.html) | Stargate serves as a data gateway positioned between client applications and a database. It features the Stargate Document API, which enables the modification and querying of data stored in the form of unstructured JSON documents within collections. A key advantage of the Document API is its schema-less nature, eliminating the need for data modeling. When integrated with Apache Cassandra, the Document API leverages Cassandra's secondary indexes for document indexing. Conversely, when used in conjunction with DataStax Enterprise, it utilizes SAI indexing for this purpose. Further insights into the architecture and storage methodologies of collections can be found in the blog post 'The Stargate Cassandra Documents API.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| [GraphQL](https://stargate.io/docs/latest/develop/graphql.html)    | API implementation for exposing Cassandra data over GraphQL. The Stargate GraphQL API is implemented to easily modify and query your table data using GraphQL types, mutations, and queries with any Cassandra deployment. <p>The Stargate GraphQL API has two modes, one developed from native GraphQL schema principles, and one developed with the Cassandra Query Language (CQL) in mind. To distinguish these two modes, the rest of the documentation will refer to the modes as schema-first and cql-first. <li>The CQL-first approach directly translates CQL tables into GraphQL types, mutations, and queries. The GraphQL schema is automatically generated from the keyspace, tables, and columns defined, but no customization is allowed. A standard set of mutations and queries are produced for searching and modifying the table data. If you are familiar with Cassandra, you might prefer this approach.<li>The schema-first approach allows you to create idiomatic GraphQL types, mutations, and queries in a manner familiar to GraphQL developers. The schema is deployed and can be updated by deploying a new schema without recreating the tables and columns directly. Under the covers, this approach will create and modify the CQL tables to match the GraphQL types. The schema can be modified for CQL decorated schema, such as specifying table and column names in Cassandra, while retaining GraphQL-centric names for the types that correspond. If you are a GraphQL developer, this approach is for you.
| [GRPC](https://stargate.io/docs/latest/develop/dev-with-grpc.html)       | Stargate is a data gateway deployed between client applications and a database. gRPC is a modern, open source remote procedure call (RPC) framework. It enables client and server applications to communicate transparently, and makes it easier to build connected systems. The Stargate gRPC API is implemented to create language-specific queries using CQL with any Cassandra deployment.

![](docs/stargate-overview.png)

## Stargate Java SDK

### QuickStart

This tutorial will guide through the steps to start coding with the Stargate SDK. Stargate nodes will be started in Docker.

> ℹ️ **You can download the code here 📥 [Download](https://github.com/DataStax-Examples/astra-samples-java-sdk/tree/main/sdk-failover-stargate)**

<img src="https://github.com/datastax/astra-sdk-java/blob/main/docs/img/quickstart-stargate.png?raw/true" />

## :clipboard: Table of content

1. [Prerequisites](#1-prerequisites)
2. [Start Stargate](#2-start-stargate)
3. [Setup your project](#3-setup-your-project)
4. [Configuration](#4-configuration)

## 1. Prerequisites

#### 📦 Docker
- Use the [reference documentation](https://www.docker.com/products/docker-desktop) to install **Docker Desktop**
- Validate your installation with

```bash
docker -v
docker run hello-world
```

#### 📦 Java Development Kit (JDK) 8+
- Use the [reference documentation](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html) to install a **Java Development Kit**
- Validate your installation with

```bash
java --version
```

#### 📦 Apache Maven
- Use the [reference documentation](https://maven.apache.org/install.html) to install **Apache Maven**
- Validate your installation with

```bash
mvn -version
```

> [🏠 Back to Table of Contents](#clipboard-table-of-content)

## 2. Start Stargate

> Elements below have been extracted from [Stargate documentation](https://stargate.io/docs/stargate/1.0/quickstart/quick_start-rest.html
)

#### ✅ Step 2a: Pull Stargate docker image `stargateio`

Download [Stargate docker image](https://hub.docker.com/r/stargateio/stargate-3_11/tags) :
![Docker Image Version (tag latest semver)](https://img.shields.io/docker/v/stargateio/stargate-3_11/v1.0.45)

```bash
docker pull stargateio/stargate-3_11:v1.0.45
```

#### ✅ Step 2b: Start Stargate container in *development* mode. (notice the environment variable `DEVELOPER_MODE=true`)

```bash
docker run --name stargate \
  -p 8080:8080 \
  -p 8081:8081 \
  -p 8082:8082 \
  -p 8090:8090 \
  -p 127.0.0.1:9042:9042 \
  -d \
  -e CLUSTER_NAME=stargate \
  -e CLUSTER_VERSION=3.11 \
  -e DEVELOPER_MODE=true \
  stargateio/stargate-3_11:v1.0.45
```

With development mode enabled, Stargate also plays the role of a data node, as such you do not need any extra Cassandra container.

Multiple ports have been declared are here what they are used for. The tools listed here (playground, swagger-ui( will be available about 40s after the docker run commmand.
- `8080` is the **Graphql** port you can access the playground on [http://localhost:8080/playground](http://localhost:8080/playground)
- `8081` is the Authentication port to retrieve a your token based on user/password
- `8082` is the Rest API port. You can access Swagger documentation on [http://localhost:8082/swagger-ui/#/](http://localhost:8082/swagger-ui/#/) also the health check is done through [http://localhost:8082/health](http://localhost:8082/health)
- `8090` is the Grpc port. A socket is open listening from Grpc calls.
- `9042` is the default CQL port. A socker is open listening CQL calls coming from the native drivers.

> [🏠 Back to Table of Contents](#clipboard-table-of-content)

## 3. Setup your project

#### ✅ Step 3a: Create the project `sdk-quickstart-stargate` with a maven archetype:

```bash
mvn archetype:generate \
  -DarchetypeGroupId=org.apache.maven.archetypes \
  -DarchetypeArtifactId=maven-archetype-quickstart \
  -DarchetypeVersion=1.4 \
  -DgroupId=com.datastax.tutorial \
  -DartifactId=sdk-quickstart-stargate \
  -Dversion=1.0.0-SNAPSHOT \
  -DinteractiveMode=false
```

#### ✅ Step 3b: Import the project favorite IDE, and replace the `pom.xml` with the following XML.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" 
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.datastax.tutorial</groupId>
  <artifactId>sdk-quickstart-stargate</artifactId>
  <version>1.0.0-SNAPSHOT</version>
  <name>sdk-quickstart-stargate</name>
  <properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <maven.compiler.source>1.8</maven.compiler.source>
    <maven.compiler.target>1.8</maven.compiler.target>
  </properties>
  <dependencies>
    <dependency>
	  <groupId>com.datastax.stargate</groupId>
	  <artifactId>stargate-sdk</artifactId>
	  <version>0.2.5</version>
    </dependency>
  </dependencies>
</project>
```

#### ✅ Step 3c: Delete folder `src/test/java`, we will experiment with a main class.

ℹ️ **Informations:**

- We removed the Junit classes generated as we will work a main class.
- We added the latest version [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.datastax.astra/astra-sdk/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.datastax.stargate/stargate-sdk/) of `stargate-sdk` dependency. The xml below may no be up-to-date.

> [🏠 Back to Table of Contents](#clipboard-table-of-content)

## 4. Configuration

#### ✅ Step 4a: Initialize Stargate client

`StargateClient` is the class you will have to work with, from there you leverage on a fluent api.

Rename `App.java` to `QuickstartStargate.java` and update the class accordingly.

```java
public static void main(String[] args) {
  try (StargateClient stargateClient = configureStargateClient()) {
    // work with Stargate
  }
}
public static StargateClient configureStargateClient() {
  return StargateClient.builder()
    .withCqlContactPoints("localhost:9042")
    .withLocalDatacenter("datacenter1")
    .withAuthCredentials("cassandra", "cassandra")
    .withApiNode(new StargateNodeConfig("127.0.0.1"))
    .build();
}
```

ℹ️ **Informations**

* Based on parameters provided in the builder, the 5 apis *(cql,rest,doc,graphQL,grpc)* will be enabled of not.

* As `CqlSession` is stateful you need to close it at the application shutdown. `StargateClient` is no different, if you enable Cql api, you need to close it at the application shutdown. To cope with this constraint the class is `Autocloseable`.

* **Cql:** needs `contact-points`, `localdatacenter` and `credentials`. If not provided the default values for contact points is `localhost:9042`

* **Https Api** need the `hostname`, `port numbers` and `credentials`. But if you are using the default ports no need to specified them.

#### ✅ Step 4b: Execute the main class `QuickstartStargate.java`

To run the application you can either use your IDE or maven

```
mvn exec:java -Dexec.main=com.datastax.tutorial.QuickstartStargate
```

**👁️ Expected output**
```bash
INFO com.datastax.stargate.sdk.StargateClient       : Initializing [StargateClient]
INFO com.datastax.stargate.sdk.StargateClient       : + Stargate nodes #[1] in [datacenter1]
INFO com.datastax.stargate.sdk.StargateClient       : + CqlSession   :[ENABLED]
INFO com.datastax.stargate.sdk.rest.ApiDataClient   : + API Data     :[ENABLED]
INFO com.datastax.stargate.sdk.doc.ApiDocumentClient: + API Document :[ENABLED]
INFO com.datastax.stargate.sdk.gql.ApiGraphQLClient : + API GraphQL  :[ENABLED]
INFO com.datastax.stargate.sdk.grpc.ApiGrpcClient . : + API Grpc     :[ENABLED]
INFO com.datastax.stargate.sdk.StargateClient       : Closing CqlSession.
```

#### ✅ Step 4c: Check you can invoke each Api. Add the following utilities methods in `QuickstartStargate.java`

```java
public static void testCqlApi(StargateClient stargateClient) {
  CqlSession cqlSession = stargateClient.cqlSession().get();
  System.out.println("Cql Version (cql)   : " + cqlSession
    .execute("SELECT cql_version from system.local")
    .one().getString("cql_version"));
}
    
public static void testRestApi(StargateClient stargateClient) {
  System.out.println("Keyspaces (rest)    : " + 
    stargateClient.apiRest()
                  .keyspaceNames()
                  .collect(Collectors.toList()));
}
    
public static void testDocumentaApi(StargateClient stargateClient) {
  System.out.println("Namespaces (doc)    : " + 
    stargateClient.apiDocument()
                  .namespaceNames()
                  .collect(Collectors.toList()));
}
    
public static void testGraphQLApi(StargateClient stargateClient) {
  System.out.println("Keyspaces (graphQL) : " + 
    stargateClient.apiGraphQL().cqlSchema().keyspaces());
}
    
public static void testGrpcApi(StargateClient stargateClient) {
  System.out.println("Cql Version (grpc)  : " + 
    stargateClient.apiGrpc()
                  .execute("SELECT cql_version from system.local")
                  .one().getString("cql_version"));
}
```

#### ✅ Step 4d: Update the `main` method accordingly:

```java
public static void main(String[] args) {
   try (StargateClient stargateClient = configureStargateClientDefault()) {
      testCqlApi(stargateClient);
      testRestApi(stargateClient);
      testDocumentaApi(stargateClient);
      testGraphQLApi(stargateClient);
      testGrpcApi(stargateClient);
   }
}
```

#### ✅ Step 4e: Execute the main class `QuickstartStargate.java` again

**👁️ Expected output**
```bash
INFO com.datastax.stargate.sdk.StargateClient       : Initializing [StargateClient]
INFO com.datastax.stargate.sdk.StargateClient       : + Stargate nodes #[1] in [datacenter1]
INFO com.datastax.stargate.sdk.StargateClient       : + CqlSession   :[ENABLED]
INFO com.datastax.stargate.sdk.rest.ApiDataClient   : + API Data     :[ENABLED]
INFO com.datastax.stargate.sdk.doc.ApiDocumentClient: + API Document :[ENABLED]
INFO com.datastax.stargate.sdk.gql.ApiGraphQLClient : + API GraphQL  :[ENABLED]
INFO com.datastax.stargate.sdk.grpc.ApiGrpcClient . : + API Grpc     :[ENABLED]
Cql Version (cql)   : 3.4.4
Keyspaces (rest)    : [system_distributed, system, data_endpoint_auth, system_schema, java, stargate_system, system_auth, system_traces]
Namespaces (doc)    : [system_distributed, system, data_endpoint_auth, system_schema, java, stargate_system, system_auth, system_traces]
Keyspaces (graphQL) : {"data":{"keyspaces":[{"name":"system_distributed"},{"name":"system"},{"name":"data_endpoint_auth"},{"name":"system_schema"},{"name":"java"},{"name":"stargate_system"},{"name":"system_auth"},{"name":"system_traces"}]}}
Cql Version (grpc)  : 3.4.4
INFO com.datastax.stargate.sdk.StargateClient       : Closing CqlSession.
```

> ℹ️ **Reminder:** You can download the code here 📥 [Download](https://github.com/DataStax-Examples/astra-samples-java-sdk/tree/main/sdk-failover-stargate)

**Congratulations:** you are ready to explore each Api leveraging the fluent api.

> [🏠 Back to Table of Contents](#clipboard-table-of-content)


This SDK *(Software Development Kit)* makes it easy to call Stargate services using idiomatic Java APIs.
                                                                                                              |

