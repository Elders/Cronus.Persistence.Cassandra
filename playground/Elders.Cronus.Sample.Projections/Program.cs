using System;
using System.Reflection;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Pipeline.Hosts;
using Elders.Cronus.Sample.Collaboration.Users.Commands;
using Elders.Cronus.Sample.IdentityAndAccess.Accounts.Commands;
using Elders.Cronus.IocContainer;
using System.Linq;
using System.Collections.Generic;
using Elders.Cronus.Projections;
using Elders.Cronus.Pipeline.Transport.RabbitMQ.Config;
using Elders.Cronus.Projections.Cassandra.Config;
using Elders.Cronus.Cluster.Config;
using Elders.Cronus.Persistence.Cassandra.Config;
using System.Configuration;
using Elders.Cronus.Projections.Versioning;
using Elders.Cronus.Transport.AzureServiceBus;

namespace Elders.Cronus.Sample.Projections
{
    class Program
    {
        static CronusHost host;
        public static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();

            //var sf = BuildSessionFactory();
            var container = new Container();
            var serviceLocator = new ServiceLocator(container);

            //var projectionTypes = typeof(UserProjection).Assembly.GetTypes().Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x));
            var systemProjections = typeof(PersistentProjectionVersionHandler).Assembly.GetTypes().Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x)).ToList();
            var collaborationProjections = typeof(Collaboration.Users.Projections.UserProjection).Assembly.GetTypes().Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x));
            systemProjections.AddRange(collaborationProjections);

            var cfg = new CronusSettings(container)
                .UseCluster(cluster =>
                {
                    cluster.ClusterName = "playground";
                    cluster.CurrentNodeName = "node1";
                })
                .UseContractsFromAssemblies(new Assembly[] { Assembly.GetAssembly(typeof(RegisterAccount)), Assembly.GetAssembly(typeof(CreateUser)) });

            var projection_serviceLocator = new ServiceLocator(container, "Projection");
            cfg.UseProjectionConsumer("Projection", consumer => consumer
                 .WithDefaultPublishers()
                 .SetNumberOfConsumerThreads(5)
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
                 .UseProjections(h => h
                     .RegisterHandlerTypes(systemProjections, projection_serviceLocator.Resolve)
                     .UseCassandraProjections(x => x
                         .SetProjectionsConnectionString("Contact Points=docker-local.com;Port=9042;Default Keyspace=cronus_sample_20180213")
                         .SetProjectionTypes(systemProjections))
                 ));

            var systemSaga_serviceLocator = new ServiceLocator(container, "SystemSaga");
            cfg.UseSagaConsumer("SystemSaga", consumer => consumer
                 .WithDefaultPublishers()
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
                 .ConfigureCassandraProjectionsStore(proj => proj
                    .SetProjectionTypes(typeof(ProjectionBuilder).Assembly)
                    .SetProjectionsConnectionString("Contact Points=docker-local.com;Port=9042;Default Keyspace=cronus_sample_20180213"))
                 .UseCassandraEventStore(eventStore => eventStore
                    .SetConnectionString(ConfigurationManager.ConnectionStrings["cronus_es"].ConnectionString)
                    .SetAggregateStatesAssembly(typeof(Elders.Cronus.Sample.Collaboration.Users.UserState)))
                 .UseSystemSagas(saga => saga.RegisterHandlerTypes(new List<Type>() { typeof(ProjectionBuilder) }, systemSaga_serviceLocator.Resolve))
                );

            //var systemProjectionTypes = typeof(PersistentProjectionVersionHandler).Assembly.GetTypes().Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x));
            //var systemProj_serviceLocator = new ServiceLocator(container, "SystemProj");
            //cfg.UseProjectionConsumer("SystemProj", consumer => consumer
            //     .WithDefaultPublishers()
            //     .UseRabbitMqTransport(x => x.Server = "docker-local.com")
            //     .UseSystemProjections(h => h
            //        .RegisterHandlerTypes(systemProjectionTypes, systemProj_serviceLocator.Resolve)
            //        .UseCassandraProjections(p => p
            //            .SetProjectionsConnectionString("Contact Points=docker-local.com;Port=9042;Default Keyspace=cronus_sample_20180213")
            //            .SetProjectionTypes(typeof(PersistentProjectionVersionHandler).Assembly)
            //                ))
            //    );

            (cfg as ISettingsBuilder).Build();
            host = container.Resolve<CronusHost>();
            host.Start();

            Console.WriteLine("Projections started");
            Console.ReadLine();
            host.Stop();
        }

        //static ISessionFactory BuildSessionFactory()
        //{
        //    var typesThatShouldBeMapped = Assembly.GetAssembly(typeof(UserProjection)).GetExportedTypes().Where(t => t.Namespace.EndsWith("DTOs"));
        //    var cfg = new NHibernate.Cfg.Configuration();
        //    Action<ModelMapper> customMappings = modelMapper =>
        //    {
        //        modelMapper.Class<User>(mapper =>
        //        {
        //            mapper.Property(pr => pr.Email, prmap => prmap.Unique(true));
        //        });
        //    };

        //    cfg = cfg.AddAutoMappings(typesThatShouldBeMapped, customMappings);
        //    cfg.CreateDatabase_AND_OVERWRITE_EXISTING_DATABASE();
        //    return cfg.BuildSessionFactory();
        //}
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

    public static class CustomeEndpointConsumerRegistrations
    {
        public static T RegisterHandlerTypes<T>(this T self, IEnumerable<Type> messageHandlers, Func<Type, object> messageHandlerFactory) where T : ISubscrptionMiddlewareSettings
        {
            self.HandlerRegistrations = messageHandlers.ToList();
            self.HandlerFactory = messageHandlerFactory;
            return self;
        }
    }
}
