namespace Elders.Cronus.Persistence.Cassandra
{
    public interface ICassandraEventStoreSettings
    {
        ICassandraProvider CassandraProvider { get; }
        ISerializer Serializer { get; }
        ITableNamingStrategy TableNameStrategy { get; }
    }
}
