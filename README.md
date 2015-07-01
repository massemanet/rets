
# rets
### a REST interface around ETS

**rets** is a CRUD service, providing network (HTTP) access to a
simple key/value store.

It has a choice of backends; in-memory (ets), and disk-resident (leveldb).

It supports structured keys, with effective selection over keys with partial
wildcards.

## EXAMPLES

Start a rets HTTP server on port 7890;
```
$ ./bin/rets.sh start

```

Create a table;
```
$ curl -s -X PUT localhost:7890/tabbe
true
```

Insert a row in a table;
```
$ curl -s -X PUT -d \"hoogashacka\" localhost:7890/tabbe/bjorn
true
```

Read a row in a table;
```
$ curl -s localhost:7890/tabbe/bjorn
"hoogashacka"
```

Get next (lexically sorted) row (key does not need to exist);
```
$ curl -s -H rets:next localhost:7890/tabbe/0
{"bjorn":"hoogashacka"}
```

Get previous (lexically sorted) row (key does not need to exist);
```
$ curl -s -H rets:prev localhost:7890/tabbe/x
{"bjorn":"hoogashacka"}
```

Insert row with structured key;
```
$ curl -s -d\"aaa\" -X put localhost:7890/tabbe/foo/bla
true
```

Insert row with structured key;
```
$ curl -s -d\"bbb\" -X put localhost:7890/tabbe/goo/bla
true
```

Read with wildcard key (request single hit, get multi hit)
```
$ curl -s localhost:7890/tabbe/_/bla
multiple_hits
```

Read with wildcard key (request and get multi hit)
```
$ curl -s -H rets:multi localhost:7890/tabbe/_/bla
{"foo/bla":"aaa","goo/bla":"bbb"}
```

Read with wildcard key (request multi hit, get single hit)
```
$ curl -s -H rets:multi localhost:7890/tabbe/foo/_
{"foo/bla":"aaa"}
```

Read with wildcard key (request and get single hit)
```
$ curl -s localhost:7890/tabbe/foo/_
"aaa"
```

Get all keys in a table;
```
$ curl -s -X GET localhost:7890/tabbe
["bjorn","foo/bla","goo/bla"]
```

Delete a row in a table;
```
$ curl -s -X DELETE localhost:7890/tabbe/bjorn
"hoogashacka"
```

Delete a table;
```
$ curl -s -X DELETE localhost:7890/tabbe
true
```

