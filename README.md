# Flink Enrichment Demo

Input:
- Kafka Topic: "transactions"
- CSV file: users.csv
- CSV file: products.csv

Output:
- Kafka Topic: "enrichedTransactions"

Sample Kafka Event Transaction:
```
{
  "id": "6a03f9a1-e19a-4fed-8c53-6af430875f9d",
  "user_id": 1002,
  "amount": 6,
  "product_id": 2002,
  "time": "2023-04-06T17:45:14.857Z"
}
```

users.csv
```
1001,kris,kris@gmail.com
1002,dave,dave@yahoo.com
1003,jan,jan@gmail.com
```

products.csv
```
2001,tea,beverages,2.55
2002,coffee,beverages,2.99
2003,dog,pets,249.99
```

Sample Kafka Enriched Transaction:
```
{
  "ts": "2023-04-06T17:45:14.857Z",
  "id": "6a03f9a1-e19a-4fed-8c53-6af430875f9d",
  "userId": "1002",
  "user": "dave",
  "email": "dave@yahoo.com",
  "productId": "2002",
  "product": "coffee",
  "category": "beverages",
  "amount": 6,
  "unitPrice": 2.99,
  "totalPrice": 17.94
}
```
