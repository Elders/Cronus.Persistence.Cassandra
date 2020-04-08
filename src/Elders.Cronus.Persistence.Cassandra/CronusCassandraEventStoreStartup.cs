using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Multitenancy;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;

namespace Elders.Cronus.Persistence.Cassandra
{
    [CronusStartup(Bootstraps.ExternalResource)]
    public class CronusCassandraEventStoreStartup : ICronusStartup
    {
        private readonly IServiceProvider serviceProvider;
        private TenantsOptions tenants;

        public CronusCassandraEventStoreStartup(IServiceProvider serviceProvider, IOptionsMonitor<TenantsOptions> tenantsOptions)
        {
            this.serviceProvider = serviceProvider;
            this.tenants = tenantsOptions.CurrentValue;
        }

        public void Bootstrap()
        {
            foreach (var tenant in tenants.Tenants)
            {
                using (var scope = serviceProvider.CreateScope())
                {
                    CronusContextFactory contextFactory = scope.ServiceProvider.GetRequiredService<CronusContextFactory>();
                    CronusContext context = contextFactory.GetContext(tenant, scope.ServiceProvider);

                    scope.ServiceProvider.GetRequiredService<ICassandraEventStoreSchema>().CreateStorage();
                }
            }
        }
    }
}
