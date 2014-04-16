using Pgshardnet;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace pgshardnet
{
    class Example
    {
        private ShardingManager GetManager()
        {
            const string dbName="dbname";
            const string userName="username";
            const string userPassword="password";

            // we have three postgresql instances = three physical shards that should work on separated machines
            // remember to create db and user in postgre sql
            var pg1 = new PhysicalShard("localhost", 5432,dbName, userName, userPassword);
            var pg2 = new PhysicalShard("localhost", 5433, dbName, userName, userPassword);
            var pg3 = new PhysicalShard("localhost", 5434, dbName, userName, userPassword);

            List<PhysicalShard> physicalShards=new List<PhysicalShard>();
            physicalShards.Add(pg1);
            physicalShards.Add(pg2);
            physicalShards.Add(pg3);

            // define logical shards. We Put three logical shards on each machine. 
            // remember to create schemes. They should be on separated tablespaces.
            List<LogicalShard> logicalShards=new List<LogicalShard>();

            logicalShards.Add(new LogicalShard() { id = 0, name = "shard0", physicalShard = pg1 });
            logicalShards.Add(new LogicalShard() { id = 1, name = "shard1", physicalShard = pg1 });
            logicalShards.Add(new LogicalShard() { id = 2, name = "shard2", physicalShard = pg1 });

            logicalShards.Add(new LogicalShard() { id = 3, name = "shard3", physicalShard = pg2 });
            logicalShards.Add(new LogicalShard() { id = 4, name = "shard4", physicalShard = pg2 });
            logicalShards.Add(new LogicalShard() { id = 5, name = "shard5", physicalShard = pg2 });

            logicalShards.Add(new LogicalShard() { id = 6, name = "shard6", physicalShard = pg3 });
            logicalShards.Add(new LogicalShard() { id = 7, name = "shard7", physicalShard = pg3 });
            logicalShards.Add(new LogicalShard() { id = 8, name = "shard8", physicalShard = pg3 });

            return new ShardingManager(physicalShards, logicalShards);
        }

        private void TestCommonQuery()
        {
            var manager = GetManager();
            using (var cmd = manager.GetCommand())
            {
                cmd.CommandText = "SELECT 1;";
                DataTable result=manager.ExecuteDataTablePhysicalCommon(cmd, true);
                // go with the result...
            }
        }



    }
}
