using System;

namespace Elders.Cronus.Persistence.Cassandra
{
    public sealed class NoKeyspaceNamingStrategy : IKeyspaceNamingStrategy
    {
        public string GetName(string baseConfigurationKeyspace)
        {
            if (baseConfigurationKeyspace.Length > 48) throw new ArgumentException($"Cassandra keyspace exceeds maximum length of 48. Keyspace: {baseConfigurationKeyspace}");

            return baseConfigurationKeyspace;
        }
    }
}
