# Query.keyspaces: [Keyspace]
            
## Example
```graphql
{
  keyspaces {
    dcs
    name
    table(name: "randomString")
    tables
    type(name: "randomString")
    types
  }
}

```