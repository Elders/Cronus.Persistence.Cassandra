namespace Elders.Cronus.Persistence.Cassandra
{
    public interface ICassandraEventStoreSettings
    {
        BoundedContext BoundedContext { get; }
        ICassandraProvider CassandraProvider { get; }
        ISerializer Serializer { get; }
        ICassandraEventStoreTableNameStrategy TableNameStrategy { get; }
    }
}