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
using Elders.Cronus.DomainModeling;
using Elders.Cronus.UnitOfWork;

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
            container.RegisterScoped<IUnitOfWork>(() => new NoUnitOfWork());
            var cfg = new CronusSettings(container)
                .UseContractsFromAssemblies(new[] { Assembly.GetAssembly(typeof(AccountRegistered)), Assembly.GetAssembly(typeof(UserCreated)) });

            string IAA = "IAA";
            var IAA_appServiceFactory = new ApplicationServiceFactory(container, IAA);
            cfg.UseCommandConsumer(IAA, consumer => consumer
                .UseRabbitMqTransport()
                .SetNumberOfConsumerThreads(5)
                .WithDefaultPublishersWithRabbitMq()
                .UseCassandraEventStore(eventStore => eventStore
                    .SetConnectionStringName("cronus_es")
                    .SetAggregateStatesAssembly(typeof(AccountState))
                    .WithNewStorageIfNotExists())
                .UseApplicationServices(cmdHandler => cmdHandler.RegisterAllHandlersInAssembly(typeof(AccountAppService), IAA_appServiceFactory.Create)));

            string COLL = "COLL";
            var COLL_appServiceFactory = new ApplicationServiceFactory(container, COLL);
            cfg.UseCommandConsumer(COLL, consumer => consumer
                .UseRabbitMqTransport()
                .WithDefaultPublishersWithRabbitMq()
                .UseCassandraEventStore(eventStore => eventStore
                    .SetConnectionStringName("cronus_es")
                    .SetAggregateStatesAssembly(typeof(UserState))
                    .WithNewStorageIfNotExists())
                .UseApplicationServices(cmdHandler => cmdHandler.RegisterAllHandlersInAssembly(typeof(UserAppService), COLL_appServiceFactory.Create)));

            (cfg as ISettingsBuilder).Build();
            host = container.Resolve<CronusHost>();
            host.Start();
        }
    }

    public class ApplicationServiceFactory
    {
        private readonly IContainer container;
        private readonly string namedInstance;

        public ApplicationServiceFactory(IContainer container, string namedInstance)
        {
            this.container = container;
            this.namedInstance = namedInstance;
        }

        public object Create(Type appServiceType)
        {
            var appService = FastActivator
                .CreateInstance(appServiceType)
                .AssignPropertySafely<IAggregateRootApplicationService>(x => x.Repository = container.Resolve<IAggregateRepository>(namedInstance));
            return appService;
        }
    }
}