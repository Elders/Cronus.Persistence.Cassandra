using System;
using Elders.Cronus.MessageProcessing;

namespace Elders.Cronus.Persistence.Cassandra
{
    public sealed class KeyspacePerTenantKeyspace : IKeyspaceNamingStrategy
    {
        private readonly ICronusContextAccessor contextAccessor;

        public KeyspacePerTenantKeyspace(ICronusContextAccessor contextAccessor)
        {
            this.contextAccessor = contextAccessor;
        }

        public string GetName(string baseConfigurationKeyspace)
        {
            var keyspace = $"{contextAccessor.CronusContext.Tenant}_{baseConfigurationKeyspace}";
            if (keyspace.Length > 48) throw new ArgumentException($"Cassandra keyspace exceeds maximum length of 48. Keyspace: {keyspace}");

            return keyspace;
        }
    }
}
