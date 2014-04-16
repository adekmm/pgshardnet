pgshardnet
==========

C# library for sharding PostgreSQL database


History
==========

We're migrating our database to PostgreSQL. We needed to split our data horizontally on several Microsoft Azure VM instances to get required amout of resources that allowe us to achieve good performance. After some test I've decided to publish this library on GitHub as it may be usefull base for many other C# applications that use PostgreSQL. implementations.


Concept
==========

1. Splits data equally on many PostgreSQL servers using modulo on bigint key. 
2. Some tables can be sharded and some may be replicated on every server for optimal querying.
3. It is designed for projects where data structure changes hardly ever. Maintaining changes on many, many shards may be painfull.


Physical and logical shards
=========

There is concept of Logical shard and Physical Shard. Physical shard is just PostgreSQL instance running on dedicated server. Logical Shard is scheme on Physical Shard that contains tables. First project that uses pgshardnet is running on 6 Azure VMs (6 physical shards). On every VM there are 3 schemes created, each on separated tablespace on separated drive. So it gives finally 18 logical shards. Every scheme can be quite easly moved to another physical server so I can scale this project up to three times before I would need to incremet  modulo and recalculate data split.


PostgreSQL ODBC vs Npgsql
=========

At the beggining I was using Npgsql. I had disconnection issues on Microsoft Azure during long queries I coudn't avoid. I switched to ODBC and set Windows Convifuration tcp keep alive options in registry for longer values. Works like a harm. The only problem is that you have to distribute PgODBC with your project.

2DO
=========

1. Write universal test. I was using my own testing environment, but it cannot be published as is project specific.
2. Maybe add option to select using ODBC or Npgsql, as it doesn't change any logic but rather only connection object.
