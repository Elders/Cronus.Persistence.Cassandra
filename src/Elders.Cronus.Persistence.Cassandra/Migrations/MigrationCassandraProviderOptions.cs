using Microsoft.Extensions.Configuration;

namespace Elders.Cronus.Persistence.Cassandra.Migrations
{
    public class MigrationCassandraProviderOptions : CassandraProviderOptions { }

    public class MigrationCassandraProviderOptionsProvider : CronusOptionsProviderBase<MigrationCassandraProviderOptions>
    {
        public const string SettingKey = "cronus:migration:source:cassandra";

        public MigrationCassandraProviderOptionsProvider(IConfiguration configuration) : base(configuration) { }

        public override void Configure(MigrationCassandraProviderOptions options)
        {
            configuration.GetSection(SettingKey).Bind(options);
        }
    }
}
