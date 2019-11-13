using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Multitenancy;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace Elders.Cronus.Persistence.Cassandra
{
    [CronusStartup(Bootstraps.ExternalResource)]
    public class CronusCassandraEventStoreStartup : ICronusStartup
    {
        private readonly IServiceProvider serviceProvider;
        private readonly ITenantList tenants;

        public CronusCassandraEventStoreStartup(IServiceProvider serviceProvider, ITenantList tenants)
        {
            this.serviceProvider = serviceProvider;
            this.tenants = tenants;
        }

        public void Bootstrap()
        {
            foreach (var tenant in tenants.GetTenants())
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
