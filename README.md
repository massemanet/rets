# rets
### a REST interface around ETS

**rets** is a CRUD service, providing network access to a simple key/value
store.
Clients can use REST-style HTTP to read, write and delete simple tables,
and rows in the tables.

Start like this;
```
./bin/rets.sh restart
```
This starts an HTTP server on port 8765.

Create a table;
```
curl -X PUT localhost:8765/tabbe
```
Insert a row in a table;
```
curl -X PUT -d "hoogashacka" localhost:8765/tabbe/bjorn
```
Delete a row in a table;
```
curl -X DELETE localhost:8765/tabbe/bjorn
```
Delete a table;
```
curl -X DELETE localhost:8765/tabbe
```
Get a row of data;
```
curl -X GET localhost:8765/tabbe/bjorn
```
Get all data in a table;
```
curl -X GET localhost:8765/tabbe
```
