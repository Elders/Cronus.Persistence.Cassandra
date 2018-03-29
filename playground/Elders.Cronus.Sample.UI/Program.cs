using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using Elders.Cronus.IocContainer;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Pipeline.Hosts;
using Elders.Cronus.Pipeline.Transport.RabbitMQ.Config;
using Elders.Cronus.Projections;
using Elders.Cronus.Projections.Cassandra.Config;
using Elders.Cronus.Projections.Versioning;
using Elders.Cronus.Sample.Collaboration.Users;
using Elders.Cronus.Sample.Collaboration.Users.Commands;
using Elders.Cronus.Sample.IdentityAndAccess.Accounts;
using Elders.Cronus.Sample.IdentityAndAccess.Accounts.Commands;
using Elders.Cronus.Serializer;
using Elders.Cronus.Transport.AzureServiceBus;
using Elders.Cronus.Transport.RabbitMQ;

namespace Elders.Cronus.Sample.UI
{
    class Program
    {
        static IPublisher<ICommand> commandPublisher;

        static Queue<StringTenantId> sentCommands = new Queue<StringTenantId>();

        static void Main(string[] args)
        {
            //Thread.Sleep(10000);
            //var asd = new Microsoft.Azure.Management.ServiceBus.ServiceBusManagementClient((Microsoft.Rest.ServiceClientCredentials)null, (System.Net.Http.DelegatingHandler[])null);
            ConfigurePublisher();
            //commandPublisher.Publish(new ReplayProjection(new ProjectionArId("e588e9ee-ef50-4e02-ac83-189adca51a6c"), null));

            //commandPublisher.Publish(new CancelReplayProjection(new ProjectionArId("e588e9ee-ef50-4e02-ac83-189adca51a6c"), null));

            HostUI(/////////////////////////////////////////////////////////////////
                                publish: SingleCreationCommandFromUpstreamBC,
                    delayBetweenBatches: 1000,
                              batchSize: 1,
                 numberOfMessagesToSend: Int32.MaxValue
                 ///////////////////////////////////////////////////////////////////
                 );

            Console.WriteLine("Done");
            Console.ReadLine();
        }

        static Container container = new Container();
        private static void ConfigurePublisher()
        {
            log4net.Config.XmlConfigurator.Configure();

            var cfg = new CronusSettings(container)
                .UseContractsFromAssemblies(new Assembly[] { Assembly.GetAssembly(typeof(RegisterAccount)), Assembly.GetAssembly(typeof(CreateUser)), Assembly.GetAssembly(typeof(ProjectionVersionManagerId)) })
                .UseAzureServiceBusTransport(x =>
                {
                    x.ClientId = "162af3b1-ed60-4382-8ce8-a1199e0b5c31";
                    x.ClientSecret = "Jej7RF6wTtgTOoqhZokc+gROk2UovFaL+zG1YF2/ous=";
                    x.ResourceGroup = "mvclientshared.integration.all";
                    x.SubscriptionId = "b12a87ce-85b9-4780-afac-cc4295574db4";
                    x.TenantId = "a43960df-8c6f-4854-8628-7f61120c33f8";
                    x.ConnectionString = "Endpoint=sb://mvclientshared-integration-all-srvbus-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=BQNROS3Pw8i5YIsoAclpWbkgHrZvUdPqlJdS/RCVc9c=";
                    x.Namespace = "mvclientshared-integration-all-srvbus-namespace";
                });
            //.UseRabbitMqTransport(x => x.Server = "docker-local.com");

            var collaborationProjections = typeof(Collaboration.Users.Projections.UserProjection).Assembly.GetTypes().Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x));
            cfg.ConfigureCassandraProjectionsStore(x => x
                .SetProjectionsConnectionString("Contact Points=docker-local.com;Port=9042;Default Keyspace=cronus_sample_20180213")
                .SetProjectionTypes(collaborationProjections));

            (cfg as ISettingsBuilder).Build();

            var serializer = container.Resolve<ISerializer>();
            commandPublisher = (container.Resolve<ITransport>() as AzureBusTransport).GetPublisher<ICommand>(serializer);
        }

        private static AccountId SingleCreationCommandFromUpstreamBC(int index)
        {
            AccountId accountId = new AccountId(Guid.NewGuid());
            var email = String.Format("cronus_{0}_{1}_@Elders.com", index, DateTime.Now);
            commandPublisher.Publish(new RegisterAccount(accountId, email));

            return accountId;
        }

        private static void SingleCreationCommandFromDownstreamBC(int index)
        {
            UserId userId = new UserId(Guid.NewGuid().ToString());
            var email = String.Format("cronus_{0}_@Elders.com", index);
            commandPublisher.Publish(new CreateUser(userId, email));
        }

        private static void SingleCreateWithMultipleUpdateCommands(int index)
        {
            AccountId accountId = new AccountId(Guid.NewGuid());
            var email = String.Format("cronus_{0}_@Elders.com", index);
            commandPublisher.Publish(new RegisterAccount(accountId, email));
            commandPublisher.Publish(new ChangeAccountEmail(accountId, email, String.Format("cronus_{0}_{0}_@Elders.com", index)));
            commandPublisher.Publish(new ChangeAccountEmail(accountId, email, String.Format("cronus_{0}_{0}_{0}_@Elders.com", index)));
            commandPublisher.Publish(new ChangeAccountEmail(accountId, email, String.Format("cronus_{0}_{0}_{0}_{0}_@Elders.com", index)));
            commandPublisher.Publish(new ChangeAccountEmail(accountId, email, String.Format("cronus_{0}_{0}_{0}_{0}_{0}_@Elders.com", index)));
            commandPublisher.Publish(new ChangeAccountEmail(accountId, email, String.Format("cronus_{0}_{0}_{0}_{0}_{0}_{0}_@Elders.com", index)));
        }

        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        private static void HostUI(Func<int, StringTenantId> publish, int delayBetweenBatches = 0, int batchSize = 1, int numberOfMessagesToSend = Int32.MaxValue)
        {
            Console.WriteLine("Start sending commands...");

            IProjectionRepository repo = container.Resolve<IProjectionRepository>();

            for (int i = 0; i <= numberOfMessagesToSend - batchSize; i = i + batchSize)
            {
                for (int j = 0; j < batchSize; j++)
                {
                    sentCommands.Enqueue(publish(i + j));
                }

                Thread.Sleep(delayBetweenBatches);

                while (sentCommands.Count > 0)
                {
                    var theId = sentCommands.Dequeue();
                    var userId = new UserId(theId.Id);
                    var result = repo.Get<Collaboration.Users.Projections.UserProjection>(userId);

                    Console.WriteLine(result.Success + "     " + userId.ToString());
                }
            }
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
