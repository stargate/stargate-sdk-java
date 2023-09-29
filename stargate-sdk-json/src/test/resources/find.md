
#### Find: Similarity Search

```json
{
  "find": {
    "sort": {
      "$vector": [
        0.15,
        0.1,
        0.1,
        0.35,
        0.55
      ]
    },
    "options": {
      "limit": 100
    }
  }
}
```

#### Find: Similarity Search + Projection

```json
{
  "find": {
    "sort": {
      "$vector": [
        0.15,
        0.1,
        0.1,
        0.35,
        0.55
      ]
    },
    "projection": {
      "$vector": 1,
      "$similarity": 1
    },
    "options": {
      "limit": 100
    }
  }
}
```

#### Find Equalities

```json
{
  "find": {
    "filter": {
      "seller": {
        "$eq": {
          "name": "Jon",
          "location": "New York"
        }
      }
    }
  }
}
```

#### Find ONE: Similarity Search

```json
{
  "findOne": {
    "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
    "projection" : {"$vector" : 1}
  }
}
```

#### Find with size

```json
{
  "find": {
    "filter": {
      "items": {
        "$size": 3
      }
    }
  }
}
```

#### Find with all

```json
{
  "find": {
    "filter": {
      "items": {
        "$all": [
          {
            "car": "Tesla",
            "color": "White"
          },
          "Extended warranty - 10 years"
        ]
      }
    }
  }
}
```

#### Find with exist

```json
{
  "find": {
    "filter": {
      "preferred_customer": {
        "$exists": true
      }
    }
  }
}
```