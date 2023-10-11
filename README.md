# Stargate Software Development Kit

[![License Apache2](https://img.shields.io/hexpm/l/plug.svg)](http://www.apache.org/licenses/LICENSE-2.0)

## Overview

This SDK *(Software Development Kit)* makes it easy to call Stargate services using idiomatic Java APIs.

## Start Locally

#### ✅ Clone the repository

```bash
git clone https://github.com/stargate/stargate-sdk-java.git
```

#### ✅ Start Stargate locally with `docker-compose`

```bash
cd stargate-sdk-java
docker-compose -f ./stargate-sdk-test/src/test/resources/docker-compose.yml up -d
```

## Sample Code

#### ✅ Using Rest API

More [sample codes](https://github.com/stargate/stargate-sdk-java/tree/main/stargate-sdk-test/src/main/java/io/stargate/sdk/test/rest) can be found here.
```java
// Initialization of rest API (default settings)
StargateRestApiClient restApiClient = new StargateRestApiClient();

// Create a keyspace
KeyspaceClient demoKS = restApiClient.keyspace("demo_ks");
if (!demoKS.exist()){
  demoKS.createSimple(1);
}

// Create a table in the keyspace
TableClient demoTable = demoKS.table("demo_table");
CreateTable tcr = new CreateTable();
tcr.setName("demo_table");
tcr.setIfNotExists(true);
tcr.getColumnDefinitions().add(new ColumnDefinition("genre", "text"));
tcr.getColumnDefinitions().add(new ColumnDefinition("year", "int"));
tcr.getColumnDefinitions().add(new ColumnDefinition("title", "text"));
tcr.getPrimaryKey().getPartitionKey().add("genre");
tcr.getPrimaryKey().getClusteringKey().add("year");
tcr.getPrimaryKey().getClusteringKey().add("title");
tcr.getTableOptions().getClusteringExpression().add(new ClusteringExpression("year", Ordering.DESC));
tcr.getTableOptions().getClusteringExpression().add(new ClusteringExpression("title", Ordering.ASC));
demoTable.create(tcr);

// Insert Record
Map<String, Object> data = new HashMap<>();
data.put("genre", "Sci-Fi");
data.put("year", 1990);
data.put("title", "Test Line");
videoTable.upsert(data);

// Udpate Record
data.put("title", "title2");
demoTable.upsert(data);

// Delete a document
KeyClient document = demoTable.key("Sci-Fi", 1990);
document.delete();

// Search table (by PK)
RowResultPage res1 = demoTable.search(
        SearchTableQuery.builder()
        .where("genre").isEqualsTo("genre1")
        .withReturnedFields("title", "year").build());
```

#### ✅ Using Document API

```java
StargateDocumentApiClient docApiClient = new StargateDocumentApiClient();

// Create namespace
NamespaceClient nsClient = docApiClient.namespace("ns_temp");
nsTemp.createSimple(1);
Assertions.assertTrue(nsTemp.exist());

// Create collection
CollectionClient clientPerson = nsClient.collection("person");
clientPerson.create();
clientPerson.exist();
clientPerson.delete();

// Create document
String docId = clientPerson.create(
  new Person("loulou", "looulou", 20, new Address("Paris", 75000))
);

// Update document
clientPerson.document(docId)
            .upsert(
   new Person("loulou", "looulou", 25, new Address("Paris", 75000))
);

// read a document
Optional<Person> loulou = personClient.document(docId).find(Person.class);

// find all
Stream<Document<String>> resultsRaw = personClient.findAll();

// Search
PageableQuery query = PageableQuery.builder()
  .pageSize(2)
  .where("age")
  .isGreaterOrEqualsThan(21)
  .build();
Page<Document<Person>> results = personClient.findPage(query, Person.class);        

// delete document
clientPerson.document(docId).delete();
```

#### ✅ Using Grpc API

```java
StargateGrpcApiClient grpcClient = new StargateGrpcApiClient();
System.out.println(grpcClient
  .execute("SELECT data_center from system.local")
  .getResults()
  .get(0).get("data_center"));
```

#### ✅ Using GraphQL API

```java
StargateGraphQLApiClient graphQLApiClient = new StargateGraphQLApiClient();
GraphQLKeyspaceDDLClient cqlSchemaClient = graphQLApiClient.keyspaceDDL();
cqlSchemaClient.keyspaces().map(Keyspace::getDcs).forEach(System.out::println);
```

#### ✅ Sample configuration

Default constructor expects a single node for each with default port 
exposition. In real deployment we expect a list of nodes for each API. 
Nodes can be grouped as Datacenter. The framework will handle load balancing 
among nodes of the same datacenter and fail-over across datacenter.

Assuming 2 DC with 2 nodes in each.

```java
// DC1
ServiceHttp dc1Node1 = new ServiceHttp("dc1-n1", "http://server1:8082", "http://server1:8082/stargate/health");
ServiceHttp dc1Node2 = new ServiceHttp("dc1-n2", "http://server2:8082", "http://server2:8082/stargate/health");
TokenProvider dc1Auth = new TokenProviderHttpAuth("cassandra", "cassandra", "http://server1:8081");
ServiceDatacenter<ServiceHttp> dc1 = 
        new ServiceDatacenter<ServiceHttp>("dc1", dc1Auth, dc1Node1, dc1Node1);

// DC2
ServiceHttp dc2Node1 = new ServiceHttp("dc2-n1", "http://server3:8082", "http://server3:8082/stargate/health");
ServiceHttp dc2Node2 = new ServiceHttp("dc2-n2", "http://server4:8082", "http://server4:8082/stargate/health");
TokenProvider dc2Auth = new TokenProviderHttpAuth("cassandra", "cassandra", "http://server3:8081");
ServiceDatacenter<ServiceHttp> dc2 = 
        new ServiceDatacenter<ServiceHttp>("dc2", dc2Auth, dc2Node1, dc2Node1);

// Initialization
StargateRestApiClient restClient = 
        new StargateRestApiClient(new ServiceDeployment<ServiceHttp>(dc1, dc2));
```




