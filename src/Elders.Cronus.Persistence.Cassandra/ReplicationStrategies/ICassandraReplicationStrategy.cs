namespace Elders.Cronus.Persistence.Cassandra.ReplicationStrategies
{
    public interface ICassandraReplicationStrategy
    {
        string CreateKeySpaceTemplate(string keySpace);
    }
}
