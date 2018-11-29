using System;
using Elders.Cronus.MessageProcessing;

namespace Elders.Cronus.Persistence.Cassandra
{
    public sealed class KeyspacePerTenantKeyspace : IKeyspaceNamingStrategy
    {
        private readonly CronusContext context;

        public KeyspacePerTenantKeyspace(CronusContext context)
        {
            this.context = context;
        }

        public string GetName(string baseConfigurationKeyspace)
        {
            string tenantPrefix = string.IsNullOrEmpty(context.Tenant) ? string.Empty : $"{context.Tenant}_";
            var keyspace = $"{tenantPrefix}{baseConfigurationKeyspace}";
            if (keyspace.Length > 48) throw new ArgumentException($"Cassandra keyspace exceeds maximum length of 48. Keyspace: {keyspace}");

            return keyspace;
        }
    }
}
