using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using DataStaxCassandra = Cassandra;

namespace Elders.Cronus.Persistence.Cassandra
{
    static class CasssandraExtensions
    {
        public static void CreateKeyspace(this DataStaxCassandra.ISession session, string keyspace, ICassandraReplicationStrategy replicationStrategy)
        {
            var createKeySpaceQuery = replicationStrategy.CreateKeySpaceTemplate(keyspace);
            session.Execute(createKeySpaceQuery);
            session.ChangeKeyspace(keyspace);
        }
    }
}
