using System;
using System.Configuration;
using System.Reflection;
using Cassandra;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Serializer;
using Elders.Cronus.IocContainer;
using Elders.Cronus.EventStore;
using Elders.Cronus.EventStore.Config;

namespace Elders.Cronus.Persistence.Cassandra.Config
{
    public static class CassandraEventStoreExtensions
    {
        public static T UseCassandraEventStore<T>(this T self, Action<CassandraEventStoreSettings> configure) where T : IConsumerSettings<ICommand>
        {
            CassandraEventStoreSettings settings = new CassandraEventStoreSettings(self);
            if (configure != null)
                configure(settings);

            (settings as ISettingsBuilder).Build();
            return self;
        }

        public static T SetConnectionStringName<T>(this T self, string connectionStringName) where T : ICassandraEventStoreSettings
        {
            return self.SetConnectionString(ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString);
        }

        public static T SetConnectionString<T>(this T self, string connectionString) where T : ICassandraEventStoreSettings
        {
            var cluster = Cluster
                .Builder()
                .WithConnectionString(connectionString)
                .Build();
            self.Session = cluster.ConnectAndCreateDefaultKeyspaceIfNotExists();
            self.KeySpace = self.Session.Keyspace;

            return self;
        }

        public static T SetAggregateStatesAssembly<T>(this T self, Type aggregateStatesAssembly) where T : ICassandraEventStoreSettings
        {
            return self.SetAggregateStatesAssembly(Assembly.GetAssembly(aggregateStatesAssembly));
        }

        public static T SetAggregateStatesAssembly<T>(this T self, Assembly aggregateStatesAssembly) where T : ICassandraEventStoreSettings
        {
            self.BoundedContext = aggregateStatesAssembly.GetAssemblyAttribute<BoundedContextAttribute>().BoundedContextName;
            self.EventStoreTableNameStrategy = new TablePerBoundedContext(aggregateStatesAssembly);
            return self;
        }

        public static T WithNewStorageIfNotExists<T>(this T self) where T : ICassandraEventStoreSettings
        {
            var storageManager = new CassandraEventStoreStorageManager(self.Session, self.EventStoreTableNameStrategy);
            storageManager.CreateStorage();
            return self;
        }
    }

    public interface ICassandraEventStoreSettings : IEventStoreSettings
    {
        string ConnectionString { get; set; }
        string KeySpace { get; set; }
        ISession Session { get; set; }
        ICassandraEventStoreTableNameStrategy EventStoreTableNameStrategy { get; set; }
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

        ISession ICassandraEventStoreSettings.Session { get; set; }
    }
}