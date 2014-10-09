# rets
### a REST interface around ETS

**rets** is a CRUD service, providing network access to a simple key/value
store.
Clients can use REST-style HTTP to read, write and delete simple tables,
and rows in the tables.

Start like this;
```
erl -pa ebin/ -pa deps/*/ebin -run rets
```
This starts an HTTP server on port 7890.

Create a table;
```
curl -X PUT localhost:7890/tabbe
```

Insert a row in a table;
```
curl -X PUT -d \"hoogashacka\" localhost:7890/tabbe/bjorn
```

Read a row in a table;
```
curl localhost:7890/tabbe/bjorn
```

Get next (lexically sorted) row
```
curl -H "next:true" localhost:7890/tabbe/0
```

Get previous (lexically sorted) row
```
curl -H "prev:true" localhost:7890/tabbe/x
```

Get all keys in a table;
```
curl -X GET localhost:7890/tabbe
```

Delete a row in a table;
```
curl -X DELETE localhost:7890/tabbe/bjorn
```

Delete a table;
```
curl -X DELETE localhost:7890/tabbe
```
