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

Counters are special integer values, handled through the "counter" and
"reset" operations.

Method  URL   Header   Body      Action                  On success, returns
PUT     Key                      create size counter     null|OldVal
PUT     Key            Val       insert Key/Val          Val
PUT     Key   counter            bump Val by one         NewInt=OldInt+1
PUT     Key   reset              reset Val to zero       OldInt

GET     KeyW  sizes              get all sizes           {Key:Size}
GET     KeyW  keys               get all matching keys   [Key]
GET     KeyW  single             succeeds iff 1 Key      Val
GET     KeyW  multi              multi read Key          {Key,Val}
GET     KeyW  next               next                    {NextKey,NextVal}
GET     KeyW  prev               prev                    {PrevKey,PrevVal}

DELETE  KeyW                     delete Key              null|{Key,Value}
DELETE  KeyW           Val       delete Key iff Key/Val  null

POST                   [Op]      perform ops             null

  Key is a string, consisting of "/"-separated Elements
  Element is a non-empty string with characters from the set "a-zA-Z0-9-"
  KeyW is a key where at least one Element is an "_"
  Val is a JSON object
  Op is; {delete,Key} | {delete,Key,OldVal} |
         {insert,Key,Val} | {insert,Key,Val,OldVal}

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
