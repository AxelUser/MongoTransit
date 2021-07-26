db.createCollection("TestDatabase.ShardedTestCollection")
sh.enableSharding("TestDatabase")
sh.shardCollection("TestDatabase.ShardedTestCollection", { key1: 1 } )