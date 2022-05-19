namespace Elders.Cronus.Persistence.Cassandra
{
    public sealed class NoTableNamingStrategyStiopa : ITableNamingStrategy
    {
        public string GetName()
        {
            return "events_stiopa";
        }
    }
}
