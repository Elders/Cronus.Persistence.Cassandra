using System.Reflection;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.Persistence.Cassandra.Config;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Pipeline.Hosts;
using Elders.Cronus.Pipeline.Transport.RabbitMQ.Config;
using Elders.Cronus.Sample.Collaboration.Users;
using Elders.Cronus.Sample.Collaboration.Users.Events;
using Elders.Cronus.Sample.IdentityAndAccess.Accounts;
using Elders.Cronus.Sample.IdentityAndAccess.Accounts.Events;
using Elders.Cronus.UnitOfWork;
using Elders.Cronus.IocContainer;

namespace Elders.Cronus.Sample.ApplicationService
{
    class Program
    {
        static CronusHost host;
        static void Main(string[] args)
        {
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
                .UseContractsFromAssemblies(new[] { Assembly.GetAssembly(typeof(AccountRegistered)), Assembly.GetAssembly(typeof(UserCreated)) });

            string IAA = "IAA";
            cfg.UseCommandConsumer(IAA, consumer => consumer
                .UseRabbitMqTransport()
                .WithDefaultPublishersWithRabbitMq()
                .UseCassandraEventStore(eventStore => eventStore
                    .SetConnectionStringName("cronus_es")
                    .SetAggregateStatesAssembly(typeof(AccountState))
                    .WithNewStorageIfNotExists())
                .UseApplicationServices(cmdHandler => cmdHandler
                    .UseUnitOfWork(new UnitOfWorkFactory() { CreateBatchUnitOfWork = () => new ApplicationServiceBatchUnitOfWork(container.Resolve<IAggregateRepository>(IAA), container.Resolve<IPublisher<IEvent>>(IAA)) })
                    .RegisterAllHandlersInAssembly(typeof(AccountAppService))));

            string COLL = "COLL";
            cfg.UseCommandConsumer(COLL, consumer => consumer
                .UseRabbitMqTransport()
                .WithDefaultPublishersWithRabbitMq()
                .UseCassandraEventStore(eventStore => eventStore
                    .SetConnectionStringName("cronus_es")
                    .SetAggregateStatesAssembly(typeof(UserState))
                    .WithNewStorageIfNotExists())
                .UseApplicationServices(cmdHandler => cmdHandler
                    .UseUnitOfWork(new UnitOfWorkFactory() { CreateBatchUnitOfWork = () => new ApplicationServiceBatchUnitOfWork(container.Resolve<IAggregateRepository>(COLL), container.Resolve<IPublisher<IEvent>>(COLL)) })
                    .RegisterAllHandlersInAssembly(typeof(UserAppService))));

            (cfg as ISettingsBuilder).Build();
            host = container.Resolve<CronusHost>();
            host.Start();
        }
    }
}