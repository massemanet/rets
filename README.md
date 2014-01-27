rets, a REST interface around ETS
====

Start like this;

    ./bin/rets.sh restart

Defaults to port 8765.

Create a table;

    curl -X PUT localhost:8765/tabbe

Insert a row in a table;

    curl -X PUT -d "hoogashacka" localhost:8765/tabbe/bjorn

Delete a row in a table;

    curl -X DELETE localhost:8765/tabbe/bjorn

Delete a table;

    curl -X DELETE localhost:8765/tabbe

Get a row of data;

    curl -X GET localhost:8765/tabbe/bjorn

Get all data in a table;

    curl -X GET localhost:8765/tabbe
