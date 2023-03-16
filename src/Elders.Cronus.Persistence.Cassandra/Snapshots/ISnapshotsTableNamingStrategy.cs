namespace Elders.Cronus.Persistence.Cassandra.Snapshots
{
    public interface ISnapshotsTableNamingStrategy
    {
        string GetName();
    }
}
