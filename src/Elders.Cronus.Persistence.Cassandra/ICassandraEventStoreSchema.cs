using System.Threading.Tasks;

namespace Elders.Cronus.Persistence.Cassandra
{
    public interface ICassandraEventStoreSchema
    {
        Task CreateEventsStorageAsync();
        Task CreateIndeciesAsync();
        Task CreateSnapshotsStorageAsync();
        Task CreateStorageAsync();
    }
}
