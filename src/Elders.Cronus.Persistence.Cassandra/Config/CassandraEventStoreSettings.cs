using System;
using System.Reflection;
using Elders.Cronus.EventStore;
using Elders.Cronus.EventStore.Config;
using Elders.Cronus.IocContainer;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Serializer;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using DataStaxCassandra = Cassandra;

namespace Elders.Cronus.Persistence.Cassandra.Config
{
    public static class CassandraEventStoreExtensions
    {
        public static T UseCassandraEventStore<T>(this T self, Action<CassandraEventStoreSettings> configure) where T : ICanConfigureEventStore
        {
            CassandraEventStoreSettings settings = new CassandraEventStoreSettings(self);
            settings.SetReconnectionPolicy(new DataStaxCassandra.ExponentialReconnectionPolicy(100, 100000));
            settings.SetRetryPolicy(new DataStaxCassandra.DefaultRetryPolicy());
            settings.SetReplicationStrategy(new SimpleReplicationStrategy(1));
            settings.SetWriteConsistencyLevel(DataStaxCassandra.ConsistencyLevel.All);
            settings.SetReadConsistencyLevel(DataStaxCassandra.ConsistencyLevel.Quorum);
            configure?.Invoke(settings);

            (settings as ISettingsBuilder).Build();
            return self;
        }

        /// <summary>
        /// Set the connection string.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="connectionString">Connection string that will be used to connect to the cassandra cluster.</param>
        /// <returns></returns>
        public static T SetConnectionString<T>(this T self, string connectionString) where T : ICassandraEventStoreSettings
        {
            var builder = new DataStaxCassandra.CassandraConnectionStringBuilder(connectionString);
            if (string.IsNullOrWhiteSpace(builder.DefaultKeyspace) == false)
            {
                self.ConnectionString = connectionString.Replace(builder.DefaultKeyspace, "");
                self.SetKeyspace(builder.DefaultKeyspace);
            }
            else
            {
                self.ConnectionString = connectionString;
            }

            return self;
        }

        /// <summary>
        /// Set the keyspace.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="keyspace">Keyspace that will be used for the event store.</param>
        /// <returns></returns>
        public static T SetKeyspace<T>(this T self, string keyspace) where T : ICassandraEventStoreSettings
        {
            self.Keyspace = keyspace;
            return self;
        }

        /// <summary>
        /// Use when you want to override all the default settings. You should use a connection string without the default keyspace and use the SetKeyspace method to specify it.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="cluster">Fully configured Cassandra cluster object.</param>
        /// <returns></returns>
        public static T SetCluster<T>(this T self, DataStaxCassandra.Cluster cluster) where T : ICassandraEventStoreSettings
        {
            self.Cluster = cluster;
            return self;
        }

        /// <summary>
        /// Use to se the consistency level that is going to be used when writing to the event store.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="writeConsistencyLevel"></param>
        /// <returns></returns>
        public static T SetWriteConsistencyLevel<T>(this T self, DataStaxCassandra.ConsistencyLevel writeConsistencyLevel) where T : ICassandraEventStoreSettings
        {
            self.WriteConsistencyLevel = writeConsistencyLevel;
            return self;
        }

        /// <summary>
        /// Use to set the consistency level that is going to be used when reading from the event store.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="readConsistencyLevel"></param>
        /// <returns></returns>
        public static T SetReadConsistencyLevel<T>(this T self, DataStaxCassandra.ConsistencyLevel readConsistencyLevel) where T : ICassandraEventStoreSettings
        {
            self.ReadConsistencyLevel = readConsistencyLevel;
            return self;
        }

        /// <summary>
        /// Use to override the default reconnection policy.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="policy">Cassandra reconnection policy.</param>
        /// <returns></returns>
        public static T SetReconnectionPolicy<T>(this T self, DataStaxCassandra.IReconnectionPolicy policy) where T : ICassandraEventStoreSettings
        {
            self.ReconnectionPolicy = policy;
            return self;
        }

        /// <summary>
        /// Use to override the default retry policy.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="policy">Cassandra retry policy.</param>
        /// <returns></returns>
        public static T SetRetryPolicy<T>(this T self, DataStaxCassandra.IRetryPolicy policy) where T : ICassandraEventStoreSettings
        {
            self.RetryPolicy = policy;
            return self;
        }

        /// <summary>
        /// Use to override the default replication strategy.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="replicationStrategy">Cassandra replication strategy.</param>
        /// <returns></returns>
        public static T SetReplicationStrategy<T>(this T self, ICassandraReplicationStrategy replicationStrategy) where T : ICassandraEventStoreSettings
        {
            self.ReplicationStrategy = replicationStrategy;
            return self;
        }

        /// <summary>
        /// Set the aggregate states assembly.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="aggregateStatesAssembly">Type that is contained in your Assembly with aggregate states.</param>
        /// <returns></returns>
        public static T SetAggregateStatesAssembly<T>(this T self, Type aggregateStatesAssembly) where T : ICassandraEventStoreSettings
        {
            return self.SetAggregateStatesAssembly(Assembly.GetAssembly(aggregateStatesAssembly));
        }

        /// <summary>
        /// Set the aggregate states assembly.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="aggregateStatesAssembly">The Assembly with your aggregate states.</param>
        /// <returns></returns>
        public static T SetAggregateStatesAssembly<T>(this T self, Assembly aggregateStatesAssembly) where T : ICassandraEventStoreSettings
        {
            return self.SetAggregateStatesAssembly(aggregateStatesAssembly, aggregateStatesAssembly.GetAssemblyAttribute<BoundedContextAttribute>().BoundedContextName);
        }

        /// <summary>
        /// Set the aggregate states assembly.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="aggregateStatesAssembly">The Assembly with your aggregate states.</param>
        /// <param name="boundedContextName">The bounded context that will be used for the event store.</param>
        /// <returns></returns>
        public static T SetAggregateStatesAssembly<T>(this T self, Assembly aggregateStatesAssembly, string boundedContextName) where T : ICassandraEventStoreSettings
        {
            self.BoundedContext = boundedContextName;
            self.EventStoreTableNameStrategy = new TablePerBoundedContext(aggregateStatesAssembly);
            return self;
        }
    }

    public interface ICassandraEventStoreSettings : IEventStoreSettings
    {
        string Keyspace { get; set; }
        string ConnectionString { get; set; }
        DataStaxCassandra.Cluster Cluster { get; set; }
        DataStaxCassandra.ConsistencyLevel WriteConsistencyLevel { get; set; }
        DataStaxCassandra.ConsistencyLevel ReadConsistencyLevel { get; set; }
        DataStaxCassandra.IRetryPolicy RetryPolicy { get; set; }
        DataStaxCassandra.IReconnectionPolicy ReconnectionPolicy { get; set; }
        ICassandraEventStoreTableNameStrategy EventStoreTableNameStrategy { get; set; }
        ICassandraReplicationStrategy ReplicationStrategy { get; set; }
    }

    public class CassandraEventStoreSettings : SettingsBuilder, ICassandraEventStoreSettings
    {
        public CassandraEventStoreSettings(ISettingsBuilder settingsBuilder) : base(settingsBuilder) { }

        public override void Build()
        {
            var builder = this as ISettingsBuilder;
            ICassandraEventStoreSettings settings = this as ICassandraEventStoreSettings;

            DataStaxCassandra.Cluster cluster = null;

            if (ReferenceEquals(null, settings.Cluster))
            {
                cluster = DataStaxCassandra.Cluster
                    .Builder()
                    .WithReconnectionPolicy(settings.ReconnectionPolicy)
                    .WithRetryPolicy(settings.RetryPolicy)
                    .WithConnectionString(settings.ConnectionString)
                    .Build();
            }
            else
            {
                cluster = settings.Cluster;
            }

            var session = cluster.Connect();
            var storageManager = new CassandraEventStoreStorageManager(session, settings.Keyspace, settings.EventStoreTableNameStrategy, settings.ReplicationStrategy);
            storageManager.CreateStorage();
            session.ChangeKeyspace(settings.Keyspace);

            var eventStore = new CassandraEventStore(session, settings.EventStoreTableNameStrategy, builder.Container.Resolve<ISerializer>(), settings.WriteConsistencyLevel, settings.ReadConsistencyLevel);
            var player = new CassandraEventStorePlayer(session, settings.EventStoreTableNameStrategy, (this as IEventStoreSettings).BoundedContext, builder.Container.Resolve<ISerializer>());

            builder.Container.RegisterSingleton<IEventStore>(() => eventStore, builder.Name);
            builder.Container.RegisterSingleton<IEventStorePlayer>(() => player, builder.Name);
        }

        string IEventStoreSettings.BoundedContext { get; set; }

        string ICassandraEventStoreSettings.ConnectionString { get; set; }

        string ICassandraEventStoreSettings.Keyspace { get; set; }

        DataStaxCassandra.Cluster ICassandraEventStoreSettings.Cluster { get; set; }

        DataStaxCassandra.ConsistencyLevel ICassandraEventStoreSettings.WriteConsistencyLevel { get; set; }

        DataStaxCassandra.ConsistencyLevel ICassandraEventStoreSettings.ReadConsistencyLevel { get; set; }

        DataStaxCassandra.IRetryPolicy ICassandraEventStoreSettings.RetryPolicy { get; set; }

        DataStaxCassandra.IReconnectionPolicy ICassandraEventStoreSettings.ReconnectionPolicy { get; set; }

        ICassandraEventStoreTableNameStrategy ICassandraEventStoreSettings.EventStoreTableNameStrategy { get; set; }

        ICassandraReplicationStrategy ICassandraEventStoreSettings.ReplicationStrategy { get; set; }
    }
}
