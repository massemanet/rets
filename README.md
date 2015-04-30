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

```
Method  URL     Header   Body      Action                  On success, returns
GET     ""                         all table_info          [{Table,Info}]
GET     T                          table_info              [{T,Info}]
GET     T/KeyW  single             succeeds iff 1 Key      Val
GET     T/KeyW  multi              multi read Key          [{Key,Val}]
GET     T/KeyW  next               next                    {NextKey,NextVal}
GET     T/KeyW  prev               prev                    {PrevKey,PrevVal}
GET     T/KeyW  keys               get all matching keys   [Key]

PUT     T                          create T                null
PUT     T/Key            Val       ins Key/Val if not Key  null
PUT     T/Key   force    Val       insert Key/Val          OldVal
PUT     T/Key            [NV,OV]   ins Key/NV iff Key/OV   OldVal
PUT     T/Key   bump               bump Val by one         NewInt=OldInt+1
PUT     T/Key   reset              reset Val to zero       OldInt

POST            write    [WriteOp] do mutating ops         null
POST            read     [ReadOp]  do read ops             [{Key,Val}|Val]

DELETE  T                          delete T iff empty      null
DELETE  T       force              delete T                null
DELETE  T/Key   force              delete Key              null|Value
DELETE  T/Key            Val       delete Key iff Key/Val  null
```

Key is a string, consisting of "/"-separated Elements.

Element is a non-empty string with characters from the set "a-zA-Z0-9-_.~"
Element can not be ".".

KeyW is a string, consisting of "/"-separated Fields. A Field is an Element or
".". The single period is a wildcard, and matches any Element.

Val is a JSON object.

WriteOp is; ["delete",Key] |
            ["delete",Key,OldVal] |
            ["insert",Key,Val] |
            ["insert",Key,Val,OldVal] |
            ["bump",Key] | ["reset",Key]

ReadOp is; ["single",KeyW] |
           ["multi",KeyW] |
           ["next",KeyW] |
           ["prev",KeyW] |
           ["keys",KeyW]
