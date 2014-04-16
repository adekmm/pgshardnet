using Pgshardnet.Exceptions;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Polly;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Data.Odbc;
using System.Diagnostics;


namespace Pgshardnet
{
    public class ShardingManager
    {

        LogicalShard[] shards;
        List<PhysicalShard> physicalShards;

        public enum ShardSelector { OneRandomShard, AllShards };

        public static string Conv<T>(T obj, bool dontConvertNumeric=false, int stringLengthCrop=0)
        {
            if (obj == null)
            {
                return "NULL";
            }
            if (!dontConvertNumeric)
            {
                if (typeof(decimal) == obj.GetType())
                {
                    decimal value = (decimal)(object)obj;
                    return Math.Round((Math.Round(value, 2) * 100), 0).ToString();

                }
                if (typeof(double) == obj.GetType())
                {
                    double value = (double)(object)obj;
                    return Math.Round((Math.Round(value, 2) * 100), 0).ToString();

                }
                if (typeof(float) == obj.GetType())
                {
                    float value = (float)(object)obj;
                    return Math.Round((Math.Round(value, 2) * 100), 0).ToString();
                }
            }
            if (typeof(bool) == obj.GetType())
            {

                var value = ((bool)(object)obj);
                if (value) { return "TRUE"; }
                else { return "FALSE"; }

            }
            if (typeof(DateTime) == obj.GetType())
            {

                var value = ((DateTime)(object)obj);
                return value.ToString("yyyy-MM-dd HH:mm:ss");


            }
            if (typeof(string) == obj.GetType() || typeof(String)==obj.GetType())
            {
                string value = (string)(object)obj;
                if (stringLengthCrop > 0 && value.Length > stringLengthCrop) value = value.Substring(0, stringLengthCrop);
                string newValue = value;
                if (value.Contains("'"))
                {
                    do
                    {
                        value = value.Replace("''", "'");
                    } while (value.Contains("''"));
                    newValue= value.Replace("'", "''");
                }
                else { newValue= value; }
                return newValue;
            }

            else
            {
                return obj.ToString();
            }
        }

        public LogicalShard[] GetLogicalShards() { return shards; }
        public List<PhysicalShard> GetPhysicalShards() { return physicalShards; }

        public ShardingManager(List<PhysicalShard> physicalShards, List<LogicalShard> logicalShards)
        {
            this.shards = logicalShards.ToArray();
            this.physicalShards = physicalShards;
            ThreadPool.SetMinThreads(shards.Length, shards.Length);
        }



        public LogicalShard GetShardByshardingKey(long shardingKey)
        {
            return shards[shardingKey % shards.Length];
        }

        public IDbConnection GetConnectionByshardingKey(long shardingKey)
        {
            return shards[shardingKey % shards.Length].physicalShard.GetConnection();
        }

        public IDbCommand GetCommand()
        {
            return new OdbcCommand();
        }

        public int ExecuteNonQueryShard(long shardingKey, IDbCommand command)
        {
            using (var conn = GetConnectionByshardingKey(shardingKey))
            {
                command.Connection = conn;
                command.CommandText = SchemaSelect(shardingKey) + command.CommandText;
                OpenConnection(conn);
                return command.ExecuteNonQuery();
            }
        }

        public int ExecuteNonQueriesShard(long shardingKey, List<IDbCommand> commands)
        {
            int result = -1;
            using (var conn = GetConnectionByshardingKey(shardingKey))
            {
                OpenConnection(conn);
                using (var tran = conn.BeginTransaction())
                {
                    using (var command = conn.CreateCommand())
                    {
                        command.Connection = conn;
                        command.Transaction = tran;

                        foreach (var inputCommand in commands)
                        {
                            try
                            {
                                command.CommandText = SchemaSelect(shardingKey) + inputCommand.CommandText;
                                result += command.ExecuteNonQuery();
                            }
                            catch (Exception e)
                            {
                                if (tran != null) tran.Rollback();
                                throw;
                            }
                        }

                    }
                    tran.Commit();
                }
                conn.Close();
            }
            return result;
        }

        public int ExecuteNonQueryShard(LogicalShard shard, IDbCommand command)
        {
            using (var conn = shard.physicalShard.GetConnection())
            {
                OpenConnection(conn);
                command.Connection = conn;
                command.CommandText = SchemaSelectByShard(shard.id) + command.CommandText;
                int res=command.ExecuteNonQuery();
                return res;
            }
        }

        public int ExecuteNonQueryAllShards(IDbCommand command)
        {
            var exceptions = new ConcurrentQueue<Exception>();
            ConcurrentStack<int> result = new ConcurrentStack<int>();
            var lshards = GetLogicalShards();
            var pshards = GetPhysicalShards();

            var state = new ParallelOptions();
            state.MaxDegreeOfParallelism = pshards.Count();
            // iterate thru phycical shards in parallel. 
            Parallel.ForEach(pshards, state, physicalshard =>
            {
                try
                {

                    // select all logical shards in physical one
                    var shardsByPhysical = lshards.Where(x => x.physicalShard == physicalshard).ToArray();
                    for (int i = 0; i < shardsByPhysical.Length; i++)
                    {
                        var lshard = shardsByPhysical[i];
                        using (var conn = physicalshard.GetConnection())
                        {
                            OpenConnection(conn);
                            var insideCommand = conn.CreateCommand();
                            insideCommand.CommandText = SchemaSelectByShard(lshard.id) + command.CommandText;
                            insideCommand.Parameters.AddRange(CloneParameters(command));
                            insideCommand.CommandTimeout = command.CommandTimeout;
                            int queryResult = insideCommand.ExecuteNonQuery();
                            result.Push(queryResult);
                        }

                    }
                }
                catch (Exception e)
                {
                    exceptions.Enqueue(e);
                }
            });
            if (exceptions.Count > 0)
            {
                throw new AggregateException(exceptions);
            }
            return result.Sum();
        }

        public int ExecuteNonQueryPhysicalCommon(IDbCommand command, bool forceIntegrity = true)
        {
            var exceptions = new ConcurrentQueue<Exception>();

            object _lock = new object();
            List<WaitHandle> handles = new List<WaitHandle>();
            List<int> resultList = new List<int>();
            var state = new ParallelOptions();
            state.MaxDegreeOfParallelism = physicalShards.Count();

            List<dynamic> shardsToExecute = new List<dynamic>();
            foreach (var physShard in physicalShards)
            {
                ManualResetEvent e = new ManualResetEvent(false);
                handles.Add(e);
                shardsToExecute.Add(new { resetEvent = e, shard = physShard });
            }

            Parallel.ForEach(shardsToExecute, state, item =>
            {
                try
                {
                    PhysicalShard physicalshard = item.shard;
                    ManualResetEvent e = item.resetEvent;
                    using (var conn = physicalshard.GetConnection())
                    {
                        OpenConnection(conn);
                        using (var transaction = conn.BeginTransaction())
                        {
                            var insideCommand = conn.CreateCommand();
                            insideCommand.CommandText = SchemaSelectCommon() + command.CommandText;
                            insideCommand.Parameters.AddRange(CloneParameters(command));
                            insideCommand.CommandTimeout = command.CommandTimeout;
                            insideCommand.Transaction = transaction;
                            var affectedCount = insideCommand.ExecuteNonQuery();
                            resultList.Add(affectedCount);

                            // commiting only after all shards executed properly
                            e.Set();
                            if (WaitHandle.WaitAll(handles.ToArray(), command.CommandTimeout * 1000))
                            {
                                if (!forceIntegrity)
                                {
                                    transaction.Commit();
                                }
                                else
                                {
                                    // commit only if all results are equal
                                    if (!resultList.Any(o => o != resultList[0]))
                                    {
                                        transaction.Commit();
                                    }
                                    else
                                    {
                                        transaction.Rollback();
                                        throw new ShardedTransactionCommitException(String.Format("Affected rows number is not equal in all shards so it will be rolled back. \nQuery: {0}", command.CommandText));
                                    }
                                }
                            }
                            else
                            {
                                transaction.Rollback();
                                throw new ShardedTransactionCommitException(String.Format("Query timed out on least one shard so it will be rolled back. \nQuery: {0}", command.CommandText));
                            }
                        }
                        conn.Close();
                    }
                }
                catch (Exception e)
                {
                    exceptions.Enqueue(e);
                }
            });

            if (exceptions.Count > 0)
            {
                throw new AggregateException(exceptions);
            }
            else
            {
                return resultList[0];
            }


        }


        public object ExecuteScalarShard(long shardingKey, IDbCommand command)
        {
            using (var conn = GetConnectionByshardingKey(shardingKey))
            {
                command.Connection = conn;
                command.CommandText = SchemaSelect(shardingKey) + command.CommandText;
                OpenConnection(conn);
                return command.ExecuteScalar();
            }
        }

        public object ExecuteScalarShard(LogicalShard shard, IDbCommand command)
        {
            using (var conn = shard.physicalShard.GetConnection())
            {
                command.Connection = conn;
                command.CommandText = SchemaSelectByShard(shard.id) + command.CommandText;
                OpenConnection(conn);
                return command.ExecuteScalar();
            }
        }

        public List<DataTable> ExecuteDataTableAllShards(IDbCommand command)
        {
            var exceptions = new ConcurrentQueue<Exception>();

            var result = new ConcurrentStack<DataTable>();
            List<Task> tasks = new List<Task>();

            foreach (var lshard in shards)
            {
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    try
                    {
                        Stopwatch sw = new Stopwatch();
                        sw.Start();
                        using (var conn = lshard.physicalShard.GetConnection())
                        {
                            OpenConnection(conn);
                            var insideCommand = conn.CreateCommand();
                            insideCommand.CommandText = SchemaSelectByShard(lshard.id) + command.CommandText;
                            insideCommand.Parameters.AddRange(CloneParameters(command));
                            insideCommand.CommandTimeout = command.CommandTimeout;

                            using (var reader = insideCommand.ExecuteReader())
                            {
                                var dt = new DataTable();
                                dt.Load(reader);
                                result.Push(dt);
                            }
                            conn.Close();
                        }

                    }
                    catch (Exception e)
                    {
                        exceptions.Enqueue(e);
                    }
                }));
            }

            Task.WaitAll(tasks.ToArray());

            if (exceptions.Count > 0)
            {
                throw new AggregateException(exceptions);
            }
            return result.ToList();
        }

        public DataTable ExecuteDataTablePhysicalCommon(IDbCommand command, bool checkIntegrity = true)
        {
            var exceptions = new ConcurrentQueue<Exception>();

            var _lockObj = new object();
            var result = new List<DataTable>();
            var state = new ParallelOptions();
            state.MaxDegreeOfParallelism = physicalShards.Count();

            // when checking integrity, we will ask all shards and compare results. If not, ask just random shard.
            List<PhysicalShard> physicalShardToQuery;
            if (checkIntegrity)
            {
                physicalShardToQuery = new List<PhysicalShard>(physicalShards);
            }
            else
            {
                physicalShardToQuery = new List<PhysicalShard>();
                Random rnd = new Random();
                int randomShardId = rnd.Next(physicalShards.Count());
                physicalShardToQuery.Add(physicalShards[randomShardId]);
            }

            Parallel.ForEach(physicalShards, state, physicalshard =>
            {
                try
                {
                    using (var conn = physicalshard.GetConnection())
                    {

                        OpenConnection(conn);
                        var insideCommand = conn.CreateCommand();
                        insideCommand.CommandText = SchemaSelectCommon() + command.CommandText;
                        insideCommand.Parameters.AddRange(CloneParameters(command));
                        insideCommand.CommandTimeout = command.CommandTimeout;
                        using (var reader = insideCommand.ExecuteReader())
                        {
                            var dt = new DataTable("Result"); //name is required for xml serialized used below
                            dt.Load(reader);
                            lock (_lockObj)
                            {
                                result.Add(dt);
                            }
                        }
                        conn.Close();
                    }
                }
                catch (Exception e)
                {
                    exceptions.Enqueue(e);
                }
            });

            if (exceptions.Count > 0)
            {
                throw new AggregateException(exceptions);
            }

            if (checkIntegrity)
            {
                if (result.Count() > 0)
                {
                    List<string> hashList = new List<string>();
                    MD5 hash = MD5.Create();
                    foreach (var ds in result)
                    {
                        using (var stream = new MemoryStream())
                        {
                            ds.WriteXml(stream);
                            stream.Position = 0;
                            var hs = hash.ComputeHash(stream);
                            var str = BitConverter.ToString(hs);
                            hashList.Add(str);
                        }
                    }
                    if (hashList.Any(o => o != hashList[0]))
                    {
                        throw new DataNotIntegralException(String.Format("Query results are not equal in all shards. Query: {0}", command.CommandText));
                    }
                }
            }

            if (result.Count() > 0)
            {
                return result[0];
            }
            else
            {
                return null;
            }
        }

        public void ExecuteReaderAllShards(IDbCommand command, Action<IDataReader> callback)
        {
            var exceptions = new ConcurrentQueue<Exception>();
            var state = new ParallelOptions();
            state.MaxDegreeOfParallelism = physicalShards.Count();
            Parallel.ForEach(shards, state, lshard =>
            {
                try
                {
                    using (var conn = lshard.physicalShard.GetConnection())
                    {
                        OpenConnection(conn);
                        var insideCommand = conn.CreateCommand();
                        insideCommand.CommandText = SchemaSelectByShard(lshard.id) + command.CommandText;
                        insideCommand.Parameters.AddRange(CloneParameters(command));
                        insideCommand.CommandTimeout = command.CommandTimeout;
                        long rows = 0;
                        using (var reader = insideCommand.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                rows++;
                                callback(reader);
                            }

                        }
                        conn.Close();
                    }

                }
                catch (Exception e)
                {
                    exceptions.Enqueue(e);
                }
            });
            if (exceptions.Count > 0)
            {
                throw new AggregateException(exceptions);
            }
        }

        public void ExecuteReaderPhysicalCommon(IDbCommand command, Action<IDataReader> callback, ShardSelector selector=ShardSelector.OneRandomShard)
        {
            var exceptions = new ConcurrentQueue<Exception>();
            Random rnd = new Random();
            var physicalShardsToExecute = new List<PhysicalShard>();
            if (selector == ShardSelector.OneRandomShard)
            {
                int randomShardId = rnd.Next(physicalShards.Count());
                physicalShardsToExecute.Add(physicalShards[randomShardId]);
            }
            else
            {
                physicalShardsToExecute = this.physicalShards;
            }

            var state = new ParallelOptions();
            state.MaxDegreeOfParallelism = physicalShards.Count();

            Parallel.ForEach(physicalShardsToExecute, state, physicalshard =>
            {
                try
                {

                    using (var conn = physicalshard.GetConnection())
                    {
                        OpenConnection(conn);
                        var insideCommand = conn.CreateCommand();
                        insideCommand.CommandText = SchemaSelectCommon() + command.CommandText;
                        insideCommand.Parameters.AddRange(CloneParameters(command));
                        insideCommand.CommandTimeout = command.CommandTimeout;
                        using (var reader = insideCommand.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                callback(reader);
                            }

                        }
                        conn.Close();
                    }
                }
                catch (Exception e)
                {
                    exceptions.Enqueue(e);
                }
            });
            if (exceptions.Count > 0)
            {
                throw new AggregateException(exceptions);
            }
        }



        private string SchemaSelect(long shardingKey)
        {
            return String.Format("SET search_path TO {0};", shards[shardingKey % shards.Length].name);
        }

        private string SchemaSelectByShard(int shardId)
        {
            return String.Format("SET search_path TO {0};", shards[shardId].name);
        }

        private string SchemaSelectCommon()
        {
            return String.Format("SET search_path TO {0};", "public");
        }

        private void OpenConnection(IDbConnection conn)
        {
            var policy = Policy
                          .Handle<OdbcException>(ex => ex.InnerException is System.TimeoutException)
                          .WaitAndRetry(5, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));

            policy.Execute(() =>
            {
                if (conn.State == ConnectionState.Closed) conn.Open();
            });
        }

        private DbParameter[] CloneParameters(IDbCommand sourceCmd)
        {
            var parameters = sourceCmd.Parameters.Cast<DbParameter>();
            var result=new List<DbParameter>();
            foreach (DbParameter p in parameters)
            {
                var newParam = new OdbcParameter(p.ParameterName, p.DbType);
                newParam.Value = p.Value;
                newParam.Direction = p.Direction;
                newParam.Size = p.Size;
                result.Add(newParam);
            }
            return result.ToArray();
        }

    }
}
