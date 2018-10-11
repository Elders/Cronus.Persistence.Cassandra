using System;
using System.Collections.Generic;
using System.Linq;
using Elders.Cronus.EventStore;
using Elders.Cronus.IocContainer;
using Elders.Cronus.Multitenancy;
using Elders.Cronus.Persistence.Cassandra.Config;
using Elders.Cronus.Pipeline.Config;
using DataStaxCassandra = Cassandra;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreFactory : IEventStoreFactory
    {
        readonly Dictionary<string, IEventStore> tenantStores;

        readonly Dictionary<string, IEventStorePlayer> tenantPlayers;

        readonly ISettingsBuilder builder;

        readonly ICassandraEventStoreSettings settings;
        private readonly ITenantList tenants;
        readonly bool hasTenantsDefined = false;
        const string NoTenantName = "notenant";

        public CassandraEventStoreFactory(Pipeline.Config.ISettingsBuilder builder, ICassandraEventStoreSettings settings, ITenantList tenants)
        {
            if (ReferenceEquals(null, builder) == true) throw new ArgumentNullException(nameof(builder));
            if (ReferenceEquals(null, settings) == true) throw new ArgumentNullException(nameof(settings));

            this.settings = settings;
            this.tenants = tenants;
            this.builder = builder;
            tenantStores = new Dictionary<string, IEventStore>();
            tenantPlayers = new Dictionary<string, IEventStorePlayer>();
            hasTenantsDefined = tenants.GetTenants().Count() > 1 || (tenants.GetTenants().Count() == 1 && tenants.GetTenants().Any(t => t.Equals(CronusAssembly.EldersTenant) == false));
            if (hasTenantsDefined)
            {
                foreach (var tenant in tenants.GetTenants())
                {
                    InitializeTenant(tenant);
                }
            }
            else
            {
                InitializeTenant(NoTenantName);
            }
        }

        public IEventStore GetEventStore(string tenant)
        {
            if (string.IsNullOrEmpty(tenant)) throw new ArgumentNullException(nameof(tenant));
            string registeredTenant = hasTenantsDefined ? tenant : NoTenantName;
            if (tenantStores.ContainsKey(registeredTenant) == false)
                throw new Exception($"EventStore for tenant {tenant} is not registered. Make sure that the tenant is registered in ");

            return tenantStores[registeredTenant];
        }

        public IEventStorePlayer GetEventStorePlayer(string tenant)
        {
            if (string.IsNullOrEmpty(tenant) == true) throw new ArgumentNullException(nameof(tenant));
            string registeredTenant = hasTenantsDefined ? tenant : NoTenantName;
            if (tenantStores.ContainsKey(registeredTenant) == false)
                throw new Exception($"EventStore for tenant {tenant} is not registered. Make sure that the tenant is registered in ");

            return tenantPlayers[registeredTenant];
        }

        void InitializeTenant(string tenant)
        {
            if (string.IsNullOrEmpty(tenant)) throw new ArgumentNullException(nameof(tenant));

            string tenantPrefix = hasTenantsDefined ? $"{tenant}_" : string.Empty;
            var keyspace = $"{tenantPrefix}{settings.Keyspace}";
            if (keyspace.Length > 48) throw new ArgumentException($"Cassandra keyspace exceeds maximum length of 48. Keyspace: {keyspace}");

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
            var storageManager = new CassandraEventStoreStorageManager(session, keyspace, settings.EventStoreTableNameStrategy, settings.ReplicationStrategy);
            storageManager.CreateStorage();
            session.ChangeKeyspace(keyspace);
            var serializer = builder.Container.Resolve<ISerializer>();
            string bc = (this.settings as EventStore.Config.IEventStoreSettings).BoundedContext;
            var eventStore = new CassandraEventStore(settings.BoundedContext, session, settings.EventStoreTableNameStrategy, serializer, settings.WriteConsistencyLevel, settings.ReadConsistencyLevel);
            var player = new CassandraEventStorePlayer(session, settings.EventStoreTableNameStrategy, bc, serializer);

            tenantStores.Add(tenant, eventStore);
            tenantPlayers.Add(tenant, player);
        }

        public IEnumerable<IEventStore> GetEventStores()
        {
            return tenantStores.Values;
        }

        public IEnumerable<IEventStorePlayer> GetEventStorePlayers()
        {
            return tenantPlayers.Values;
        }
    }
}
