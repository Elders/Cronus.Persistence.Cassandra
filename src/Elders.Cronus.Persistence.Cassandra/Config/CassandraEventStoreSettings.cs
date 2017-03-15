using System;
using System.Reflection;
using Elders.Cronus.DomainModeling;
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
        public static T UseCassandraEventStore<T>(this T self, Action<CassandraEventStoreSettings> configure) where T : IConsumerSettings<ICommand>
        {
            CassandraEventStoreSettings settings = new CassandraEventStoreSettings(self);
            configure?.Invoke(settings);

            (settings as ISettingsBuilder).Build();
            return self;
        }

        public static T SetCluster<T>(this T self, DataStaxCassandra.Cluster cluster) where T : ICassandraEventStoreSettings
        {
            self.Session = cluster.ConnectAndCreateDefaultKeyspaceIfNotExists();
            self.KeySpace = self.Session.Keyspace;
            return self;
        }

        public static T SetReplicationStrategy<T>(this T self, ICassandraReplicationStrategy replicationStrategy) where T : ICassandraEventStoreSettings
        {
            self.ReplicationStrategy = replicationStrategy;
            return self;
        }

        public static T SetAggregateStatesAssembly<T>(this T self, Type aggregateStatesAssembly) where T : ICassandraEventStoreSettings
        {
            return self.SetAggregateStatesAssembly(Assembly.GetAssembly(aggregateStatesAssembly));
        }

        public static T SetAggregateStatesAssembly<T>(this T self, Assembly aggregateStatesAssembly) where T : ICassandraEventStoreSettings
        {
            return self.SetAggregateStatesAssembly(aggregateStatesAssembly, aggregateStatesAssembly.GetAssemblyAttribute<BoundedContextAttribute>().BoundedContextName);
        }

        public static T SetAggregateStatesAssembly<T>(this T self, Assembly aggregateStatesAssembly, string boundedContextName) where T : ICassandraEventStoreSettings
        {
            self.BoundedContext = boundedContextName;
            self.EventStoreTableNameStrategy = new TablePerBoundedContext(aggregateStatesAssembly);
            return self;
        }

        public static T WithNewStorageIfNotExists<T>(this T self) where T : ICassandraEventStoreSettings
        {
            var storageManager = new CassandraEventStoreStorageManager(self.Session, self.EventStoreTableNameStrategy, self.ReplicationStrategy);
            storageManager.CreateStorage();
            return self;
        }
    }

    public interface ICassandraEventStoreSettings : IEventStoreSettings
    {
        string ConnectionString { get; set; }
        string KeySpace { get; set; }
        DataStaxCassandra.ISession Session { get; set; }
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

            var eventStore = new CassandraEventStore(settings.Session, settings.EventStoreTableNameStrategy, builder.Container.Resolve<ISerializer>());
            var player = new CassandraEventStorePlayer(settings.Session, settings.EventStoreTableNameStrategy, (this as IEventStoreSettings).BoundedContext, builder.Container.Resolve<ISerializer>());

            builder.Container.RegisterSingleton<IEventStore>(() => eventStore, builder.Name);
            builder.Container.RegisterSingleton<IEventStorePlayer>(() => player, builder.Name);
        }

        string IEventStoreSettings.BoundedContext { get; set; }

        string ICassandraEventStoreSettings.ConnectionString { get; set; }

        ICassandraEventStoreTableNameStrategy ICassandraEventStoreSettings.EventStoreTableNameStrategy { get; set; }

        string ICassandraEventStoreSettings.KeySpace { get; set; }

        DataStaxCassandra.ISession ICassandraEventStoreSettings.Session { get; set; }

        ICassandraReplicationStrategy ICassandraEventStoreSettings.ReplicationStrategy { get; set; }
    }
}
