
# CASSANDRADD

A simple tool to insert dummy data into Cassandra.

Build `cassandradd`
````
go build
````

Initialize Cassandra connection configuration:
````
cassadradd init

Make sure to have your test Keyspace ready :)

Enter cassandra host: localhost
Enter cassandra port: 7199
Enter cassandra keyspace: test_keyspace

Initialized cassandra connection configurations.
````

Start inserting dummy data
```
cassandradd run

Inserting dummy data...
Batch no.1 completed.
Successfully written 1 rows to the table.

---

cassandradd --batch 10 --size 1000 run

Inserting dummy data...
Batch no.1 completed.
Batch no.2 completed.
Batch no.3 completed.
Batch no.4 completed.
Batch no.5 completed.
Batch no.6 completed.
Batch no.7 completed.
Batch no.8 completed.
Batch no.9 completed.
Batch no.10 completed.
Successfully written 10,000 rows to the table.
```

Flags:
```
--batches / -b     : No. of batches to run.
--size / -s        : No. of inserts per batches.
--concurrency / -c : No. of concurrent inserts.
```

If you need to create your own schema, feel free to use edit the code to suit your needs :)