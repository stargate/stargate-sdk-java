# Query.keyspace: Keyspace
                 
## Arguments
| Name | Description | Required | Type |
| :--- | :---------- | :------: | :--: |
| name |  | ✅ | String! |
            
## Example
```graphql
{
  keyspace(name: "randomString") {
    dcs
    name
    table(name: "randomString")
    tables
    type(name: "randomString")
    types
  }
}

```