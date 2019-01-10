namespace Elders.Cronus.Persistence.Cassandra
{
    public interface ICassandraEventStoreSchema
    {
        void CreateEventsStorage();
        void CreateIndecies();
        void CreateSnapshotsStorage();
        void CreateStorage();
    }
}