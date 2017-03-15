using System.Configuration;
using DataStaxCassandra = Cassandra;

namespace Elders.Cronus.Persistence.Cassandra.Config
{
    public class CassandraConfiguration : ConfigurationSection
    {
        [ConfigurationProperty("connectionString")]
        public string ConnectionString
        {
            get { return this["connectionString"].ToString(); }
        }
    }
}