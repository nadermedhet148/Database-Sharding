# Database sharding approaches 

## Hash sharding
---------------------------
With (consistent) hash sharding, data is evenly and randomly distributed across shards using a partitioning algorithm. Each row of the table is placed into a shard determined by computing a consistent hash on the partition column values of that row. This is shown in the figure below.
![HashSharding](docs/tablet_hash_1.png)

## Rebalanced hash shards
---------------------
At some point in the software life, we will need to add a new node to our shards but this will make the hash values in an unbalanced state because some partitions from old shards now are related to the new node  
so we need to move them to the new node to rebalance our state 

## Range sharding
-----------------------
Range sharding involves splitting the rows of a table into contiguous ranges that respect the sort order of the table based on the primary key column values. The tables that are range sharded usually start out with a single shard. As data is inserted into the table, it is dynamically split into multiple shards because it is not always possible to know the distribution of keys in the table ahead of time. The basic idea behind range sharding is shown in the figure below.
![RangeSharding](docs/tablet_range_1.png)
