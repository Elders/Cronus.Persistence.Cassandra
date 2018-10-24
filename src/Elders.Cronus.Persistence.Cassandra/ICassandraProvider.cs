using Cassandra;

namespace Elders.Cronus.Persistence.Cassandra
{
    public interface ICassandraProvider
    {
        Cluster GetCluster();
        ISession GetSession();
        ISession GetSchemaSession();
    }
}
