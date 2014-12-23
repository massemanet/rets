# rets
### a REST interface around ETS/leveldb

**rets** is a CRUD service, providing network access to a simple key/value
store.

Keys are strings. They are structured, with "/" used to separate
elements. Elements can be wildcarded, using "." is the wildcard
character.

Reads are traditional. Several keys can be read atomically.

Writes are either unconditional, or done in CAS (Compare-and-Swap)
style (i.e. the write operation contains a key, a new, and an old
value, and the write only goes through if the key is assocoiated with
the old value).

Deletes are either unconditional or done in CAS style.

Counters are special integer values, handled through the "bump" and
"reset" operations.

Method  URL   Header   Body      Action                  On success, returns
GET     KeyW  single             succeeds iff 1 Key      Val
GET     KeyW  multi              multi read Key          [{Key,Val}]
GET     KeyW  next               next                    {NextKey,NextVal}
GET     KeyW  prev               prev                    {PrevKey,PrevVal}
GET     KeyW  keys               get all matching keys   [Key]

PUT     Key   force    Val       insert Key/Val          OldVal
PUT     Key            [NV,OV]   ins Key/NV iff Key/OV   OldVal
PUT     Key   bump               bump Val by one         NewInt=OldInt+1
PUT     Key   reset              reset Val to zero       OldInt

POST          write    [WriteOp] mutate Keys             null
POST          read     [ReadOp]  read many keys          [{Key,Val}|Val]

DELETE  Key   force              delete Key              null|Value
DELETE  Key            Val       delete Key iff Key/Val  null

  Key is a string, consisting of "/"-separated Elements
  Element is a non-empty string with characters from the set "a-zA-Z0-9-_.~"
  Element can not be "."
  KeyW is a string, consisting of "/"-separated Element | ".". The single
  period is a wildcard, and matches any Element
  Val is a JSON object
  WriteOp is; ["delete",Key] | ["delete",Key,OldVal] |
              ["insert",Key,Val] | ["insert",Key,Val,OldVal] |
              ["bump",Key] | ["reset",Key]
  ReadOp is; ["single",KeyW] | ["multi",KeyW] |
             ["next",KeyW] | ["prev",KeyW] |
             ["keys",KeyW]


## EXAMPLES
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
curl -H "rets:next" localhost:7890/tabbe/0
```

Get previous (lexically sorted) row
```
curl -H "rets:prev" localhost:7890/tabbe/x
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
