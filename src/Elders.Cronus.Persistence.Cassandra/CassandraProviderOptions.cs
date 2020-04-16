using Microsoft.Extensions.Configuration;
using System.Collections.Generic;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraProviderOptions
    {
        public CassandraProviderOptions()
        {
            Datacenters = new List<string>();
        }

        public string ConnectionString { get; set; }

        public string ReplicationStrategy { get; set; } = "simple";

        public int ReplicationFactor { get; set; } = 1;

        public List<string> Datacenters { get; set; }
    }

    public class CassandraProviderOptionsProvider : CronusOptionsProviderBase<CassandraProviderOptions>
    {
        public const string SettingKey = "cronus:persistence:cassandra";

        public CassandraProviderOptionsProvider(IConfiguration configuration) : base(configuration) { }

        public override void Configure(CassandraProviderOptions options)
        {
            configuration.GetSection(SettingKey).Bind(options);
        }
    }
}
