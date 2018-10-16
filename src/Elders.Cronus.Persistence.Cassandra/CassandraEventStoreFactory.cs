using System;
using System.Collections.Generic;
using System.Linq;
using Elders.Cronus.EventStore;
using Elders.Cronus.Multitenancy;
using Microsoft.Extensions.Configuration;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreFactory : IEventStoreFactory
    {
        private readonly Dictionary<string, IEventStore> tenantStores;

        private readonly Dictionary<string, IEventStorePlayer> tenantPlayers;

        private readonly IConfiguration configuration;
        private readonly ITenantList tenants;
        private readonly CassandraProviderForEventStore cassandraProvider;
        private readonly ISerializer serializer;
        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;
        private readonly bool hasTenantsDefined = false;
        const string NoTenantName = "notenant";

        public CassandraEventStoreFactory(IConfiguration configuration, ITenantList tenants, CassandraProviderForEventStore cassandraProvider, ISerializer serializer, ICassandraEventStoreTableNameStrategy tableNameStrategy)
        {
            if (configuration is null) throw new ArgumentNullException(nameof(configuration));
            if (tenants is null) throw new ArgumentNullException(nameof(tenants));
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            if (serializer is null) throw new ArgumentNullException(nameof(serializer));
            if (tableNameStrategy is null) throw new ArgumentNullException(nameof(tableNameStrategy));

            this.configuration = configuration;
            this.tenants = tenants;
            this.cassandraProvider = cassandraProvider;
            this.serializer = serializer;
            this.tableNameStrategy = tableNameStrategy;

            tenantStores = new Dictionary<string, IEventStore>();
            tenantPlayers = new Dictionary<string, IEventStorePlayer>();
            hasTenantsDefined = (tenants is null == false) && (tenants.GetTenants().Count() > 1 || (tenants.GetTenants().Count() == 1 && tenants.GetTenants().Any(t => t.Equals(CronusAssembly.EldersTenant) == false)));
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

            var session = cassandraProvider.GetSession(tenant);

            var storageManager = new CassandraEventStoreStorageManager(session, tableNameStrategy);
            storageManager.CreateStorage();

            string bc = configuration["cronus_boundedcontext"];
            var eventStore = new CassandraEventStore(bc, session, tableNameStrategy, serializer);
            var player = new CassandraEventStorePlayer(session, tableNameStrategy, bc, serializer);

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
