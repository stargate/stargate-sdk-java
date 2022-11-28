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

```java
// Initialization of rest API (default settings)
StargateRestApiClient restApiClient = new StargateRestApiClient();

// Create a keyspace
KeyspaceClient demoKS = restApiClient.keyspace("demo_ks");

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

// Delete a record
KeyClient record = demoTable.key("Sci-Fi", 1990);
record.delete();

// Do a search
RowResultPage res1 = demoTable.search(
        SearchTableQuery.builder()
        .where("genre").isEqualsTo("genre1")
        .withReturnedFields("title", "year").build());
```

#### ✅ Using Document API





