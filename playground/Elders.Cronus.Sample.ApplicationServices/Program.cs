using System.Reflection;
using Elders.Cronus.Persistence.Cassandra.Config;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Pipeline.Hosts;
using Elders.Cronus.Pipeline.Transport.RabbitMQ.Config;
using Elders.Cronus.Sample.Collaboration.Users;
using Elders.Cronus.Sample.Collaboration.Users.Events;
using Elders.Cronus.Sample.IdentityAndAccess.Accounts;
using Elders.Cronus.Sample.IdentityAndAccess.Accounts.Events;
using Elders.Cronus.IocContainer;
using System;
using System.Configuration;
using Elders.Cronus.Cluster.Config;
using Elders.Cronus.AtomicAction.Config;
using Elders.Cronus.Projections;
using Elders.Cronus.Cluster;
using System.Linq;
using Elders.Cronus.Projections.Versioning;
using RabbitMQ.Client;
using Elders.Cronus.Transport.AzureServiceBus;

namespace Elders.Cronus.Sample.ApplicationService
{
    class Program
    {
        static CronusHost host;
        static void Main(string[] args)
        {
            //var connStr = @"amqps://RootManageSharedAccessKey:BQNROS3Pw8i5YIsoAclpWbkgHrZvUdPqlJdS%2FRCVc9c%3D@mvclientshared-integration-all-srvbus-namespace.servicebus.windows.net";
            //var gg = new AmqpTcpEndpoint(connStr);

            //var fact = new ConnectionFactory(){ }

            log4net.Config.XmlConfigurator.Configure();
            UseCronusHostWithCassandraEventStore();
            System.Console.WriteLine("Started command handlers");
            System.Console.ReadLine();
            host.Stop();
            host = null;
        }

        static void UseCronusHostWithCassandraEventStore()
        {
            var container = new Container();
            var cfg = new CronusSettings(container)
                .UseCluster(cluster =>
                {
                    cluster.ClusterName = "playground";
                    cluster.CurrentNodeName = "node1";
                    cluster.UseAggregateRootAtomicAction(atomic => atomic.WithInMemory());
                })
                .UseContractsFromAssemblies(new[] { Assembly.GetAssembly(typeof(AccountRegistered)), Assembly.GetAssembly(typeof(UserCreated)), Assembly.GetAssembly(typeof(ProjectionVersionManagerId)) });

            string SYSTEM = "SYSTEM";
            var SYSTEM_appServiceFactory = new ServiceLocator(container, SYSTEM);
            cfg.UseCommandConsumer(SYSTEM, consumer => consumer
            .UseAzureServiceBusTransport(x =>
                {
                    x.ClientId = "162af3b1-ed60-4382-8ce8-a1199e0b5c31";
                    x.ClientSecret = "Jej7RF6wTtgTOoqhZokc+gROk2UovFaL+zG1YF2/ous=";
                    x.ResourceGroup = "mvclientshared.integration.all";
                    x.SubscriptionId = "b12a87ce-85b9-4780-afac-cc4295574db4";
                    x.TenantId = "a43960df-8c6f-4854-8628-7f61120c33f8";
                    x.ConnectionString = "Endpoint=sb://mvclientshared-integration-all-srvbus-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=BQNROS3Pw8i5YIsoAclpWbkgHrZvUdPqlJdS/RCVc9c=";
                    x.Namespace = "mvclientshared-integration-all-srvbus-namespace";
                })
                //.UseRabbitMqTransport(x => x.Server = "docker-local.com")
                .WithDefaultPublishers()
                .UseCassandraEventStore(eventStore => eventStore
                    .SetConnectionString(ConfigurationManager.ConnectionStrings["cronus_es"].ConnectionString)
                    .SetAggregateStatesAssembly(typeof(ProjectionVersionManagerAppService)))
                .UseSystemServices(cmdHandler => cmdHandler.RegisterHandlersInAssembly(new[] { typeof(ProjectionVersionManagerAppService).Assembly }, SYSTEM_appServiceFactory.Resolve))
            );

            //Endpoint = sb://mvclientshared-integration-all-srvbus-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=BQNROS3Pw8i5YIsoAclpWbkgHrZvUdPqlJdS/RCVc9c=
            string IAA = "IAA";
            var IAA_appServiceFactory = new ServiceLocator(container, IAA);
            cfg.UseCommandConsumer(IAA, consumer => consumer
            .UseAzureServiceBusTransport(x =>
                {
                    x.ClientId = "162af3b1-ed60-4382-8ce8-a1199e0b5c31";
                    x.ClientSecret = "Jej7RF6wTtgTOoqhZokc+gROk2UovFaL+zG1YF2/ous=";
                    x.ResourceGroup = "mvclientshared.integration.all";
                    x.SubscriptionId = "b12a87ce-85b9-4780-afac-cc4295574db4";
                    x.TenantId = "a43960df-8c6f-4854-8628-7f61120c33f8";
                    x.ConnectionString = "Endpoint=sb://mvclientshared-integration-all-srvbus-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=BQNROS3Pw8i5YIsoAclpWbkgHrZvUdPqlJdS/RCVc9c=";
                    x.Namespace = "mvclientshared-integration-all-srvbus-namespace";
                })
                //.UseRabbitMqTransport(x => x.Server = "docker-local.com")
                //.UseAzureServiceBus
                .SetNumberOfConsumerThreads(5)
                .WithDefaultPublishers()
                .UseCassandraEventStore(eventStore => eventStore
                    .SetConnectionString(ConfigurationManager.ConnectionStrings["cronus_es"].ConnectionString)
                    .SetAggregateStatesAssembly(typeof(AccountState)))
                .UseApplicationServices(cmdHandler => cmdHandler.RegisterHandlersInAssembly(new[] { typeof(AccountAppService).Assembly }, IAA_appServiceFactory.Resolve))
            );

            string COLL = "COLL";
            var COLL_appServiceFactory = new ServiceLocator(container, COLL);
            cfg.UseCommandConsumer(COLL, consumer => consumer
            .UseAzureServiceBusTransport(x =>
                {
                    x.ClientId = "162af3b1-ed60-4382-8ce8-a1199e0b5c31";
                    x.ClientSecret = "Jej7RF6wTtgTOoqhZokc+gROk2UovFaL+zG1YF2/ous=";
                    x.ResourceGroup = "mvclientshared.integration.all";
                    x.SubscriptionId = "b12a87ce-85b9-4780-afac-cc4295574db4";
                    x.TenantId = "a43960df-8c6f-4854-8628-7f61120c33f8";
                    x.ConnectionString = "Endpoint=sb://mvclientshared-integration-all-srvbus-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=BQNROS3Pw8i5YIsoAclpWbkgHrZvUdPqlJdS/RCVc9c=";
                    x.Namespace = "mvclientshared-integration-all-srvbus-namespace";
                })
                //.UseRabbitMqTransport(x => x.Server = "docker-local.com")
                .SetNumberOfConsumerThreads(5)
                .WithDefaultPublishers()
                .UseCassandraEventStore(eventStore => eventStore
                    .SetConnectionString(ConfigurationManager.ConnectionStrings["cronus_es"].ConnectionString)
                    .SetAggregateStatesAssembly(typeof(UserState)))
                .UseApplicationServices(cmdHandler => cmdHandler.RegisterHandlersInAssembly(new[] { typeof(UserAppService).Assembly }, COLL_appServiceFactory.Resolve))
            );

            (cfg as ISettingsBuilder).Build();
            host = container.Resolve<CronusHost>();
            host.Start();
        }
    }

    public class ServiceLocator
    {
        IContainer container;
        private readonly string namedInstance;

        public ServiceLocator(IContainer container, string namedInstance = null)
        {
            this.container = container;
            this.namedInstance = namedInstance;
        }

        public object Resolve(Type objectType)
        {
            var instance = FastActivator.CreateInstance(objectType);
            var props = objectType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).ToList();
            var dependencies = props.Where(x => container.IsRegistered(x.PropertyType, namedInstance));
            foreach (var item in dependencies)
            {
                item.SetValue(instance, container.Resolve(item.PropertyType, namedInstance));
            }
            return instance;
        }
    }

    public static class MeasureExecutionTime
    {
        public static string Start(System.Action action)
        {
            string result = string.Empty;

            var stopWatch = new System.Diagnostics.Stopwatch();
            stopWatch.Start();
            action();
            stopWatch.Stop();
            System.TimeSpan ts = stopWatch.Elapsed;
            result = System.String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            return result;
        }

        public static string Start(System.Action action, int repeat, bool showTicksInfo = false)
        {
            var stopWatch = new System.Diagnostics.Stopwatch();
            stopWatch.Start();
            for (int i = 0; i < repeat; i++)
            {
                action();
            }
            stopWatch.Stop();
            System.TimeSpan total = stopWatch.Elapsed;
            System.TimeSpan average = new System.TimeSpan(stopWatch.Elapsed.Ticks / repeat);

            System.Text.StringBuilder perfResultsBuilder = new System.Text.StringBuilder();
            perfResultsBuilder.AppendLine("--------------------------------------------------------------");
            perfResultsBuilder.AppendFormat("  Total Time => {0}\r\nAverage Time => {1}", Align(total), Align(average));
            perfResultsBuilder.AppendLine();
            perfResultsBuilder.AppendLine("--------------------------------------------------------------");
            if (showTicksInfo)
                perfResultsBuilder.AppendLine(TicksInfo());
            return perfResultsBuilder.ToString();
        }

        static string Align(System.TimeSpan interval)
        {
            string intervalStr = interval.ToString();
            int pointIndex = intervalStr.IndexOf(':');

            pointIndex = intervalStr.IndexOf('.', pointIndex);
            if (pointIndex < 0) intervalStr += "        ";
            return intervalStr;
        }

        static string TicksInfo()
        {
            System.Text.StringBuilder ticksInfoBuilder = new System.Text.StringBuilder("\r\n\r\n");
            ticksInfoBuilder.AppendLine("Ticks Info");
            ticksInfoBuilder.AppendLine("--------------------------------------------------------------");
            const string numberFmt = "{0,-22}{1,18:N0}";
            const string timeFmt = "{0,-22}{1,26}";

            ticksInfoBuilder.AppendLine(System.String.Format(numberFmt, "Field", "Value"));
            ticksInfoBuilder.AppendLine(System.String.Format(numberFmt, "-----", "-----"));

            // Display the maximum, minimum, and zero TimeSpan values.
            ticksInfoBuilder.AppendLine(System.String.Format(timeFmt, "Maximum TimeSpan", Align(System.TimeSpan.MaxValue)));
            ticksInfoBuilder.AppendLine(System.String.Format(timeFmt, "Minimum TimeSpan", Align(System.TimeSpan.MinValue)));
            ticksInfoBuilder.AppendLine(System.String.Format(timeFmt, "Zero TimeSpan", Align(System.TimeSpan.Zero)));
            ticksInfoBuilder.AppendLine();

            // Display the ticks-per-time-unit fields.
            ticksInfoBuilder.AppendLine(System.String.Format(numberFmt, "Ticks per day", System.TimeSpan.TicksPerDay));
            ticksInfoBuilder.AppendLine(System.String.Format(numberFmt, "Ticks per hour", System.TimeSpan.TicksPerHour));
            ticksInfoBuilder.AppendLine(System.String.Format(numberFmt, "Ticks per minute", System.TimeSpan.TicksPerMinute));
            ticksInfoBuilder.AppendLine(System.String.Format(numberFmt, "Ticks per second", System.TimeSpan.TicksPerSecond));
            ticksInfoBuilder.AppendLine(System.String.Format(numberFmt, "Ticks per millisecond", System.TimeSpan.TicksPerMillisecond));
            ticksInfoBuilder.AppendLine("--------------------------------------------------------------");
            return ticksInfoBuilder.ToString();
        }
    }
}
