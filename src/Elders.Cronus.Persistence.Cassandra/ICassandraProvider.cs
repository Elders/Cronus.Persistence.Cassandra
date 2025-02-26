using Cassandra;
using System.Threading.Tasks;

namespace Elders.Cronus.Persistence.Cassandra
{
    public interface ICassandraProvider
    {
        string GetKeyspace();
        Task<ICluster> GetClusterAsync();
        Task<ISession> GetSessionAsync();
    }
}
