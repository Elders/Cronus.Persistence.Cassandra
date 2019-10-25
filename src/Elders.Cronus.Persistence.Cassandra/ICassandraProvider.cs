using Cassandra;

namespace Elders.Cronus.Persistence.Cassandra
{
    public interface ICassandraProvider
    {
        ICluster GetCluster();
        ISession GetSession();
    }
}
