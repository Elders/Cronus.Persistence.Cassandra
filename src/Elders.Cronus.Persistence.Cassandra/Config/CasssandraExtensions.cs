using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using DataStaxCassandra = Cassandra;

namespace Elders.Cronus.Persistence.Cassandra.Config
{
    public static class CasssandraExtensions
    {
        internal static void CreateKeyspace(this DataStaxCassandra.ISession session, string keyspace, ICassandraReplicationStrategy replicationStrategy)
        {
            var createKeySpaceQuery = replicationStrategy.CreateKeySpaceTemplate(keyspace);
            session.Execute(createKeySpaceQuery);
            session.ChangeKeyspace(keyspace);
        }
    }
}
