using System;
using System.Reflection;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Pipeline.Hosts;
using Elders.Cronus.Pipeline.Transport.RabbitMQ.Config;
using Elders.Cronus.Sample.Collaboration.Users.Commands;
using Elders.Cronus.Sample.Collaboration.Users.Projections;
using Elders.Cronus.Sample.IdentityAndAccess.Accounts.Commands;
using Elders.Cronus.IocContainer;
using Elders.Cronus.Projections.Cassandra.Config;
using System.Linq;
using System.Collections.Generic;
using Elders.Cronus.DomainModeling.Projections;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.Projections.Cassandra.Snapshots;

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
            var projectionTypes = typeof(UserProjection).Assembly.GetTypes().Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x) == false && typeof(IProjection).IsAssignableFrom(x));

            var cfg = new CronusSettings(container)
                    .UseContractsFromAssemblies(new Assembly[] { Assembly.GetAssembly(typeof(RegisterAccount)), Assembly.GetAssembly(typeof(CreateUser)) })
                    .UseProjectionConsumer(consumer => consumer
                        .SetNumberOfConsumerThreads(1)
                        .WithDefaultPublishers()
                        .UseRabbitMqTransport(x => x.Server = "docker-local.com")
                        .UseProjections(h => h
                            .RegisterHandlerTypes(projectionTypes, serviceLocator.Resolve)
                        //.UseCassandraProjections(x => x
                        //    .SetProjectionsConnectionString("Contact Points=docker-local.com;Port=9042;Default Keyspace=cronus_sample_20150317")
                        //    //.UseSnapshots(projectionTypes)
                        //    //.UseSnapshotStrategy(new DefaultSnapshotStrategy(TimeSpan.FromDays(1), 500))
                        //    .SetProjectionTypes(projectionTypes))
                        ));

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

        public ServiceLocator(IContainer container)
        {
            this.container = container;
        }

        public object Resolve(Type objectType)
        {
            var instance = FastActivator.CreateInstance(objectType);
            var props = objectType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).ToList();
            var dependencies = props.Where(x => container.IsRegistered(x.PropertyType));
            foreach (var item in dependencies)
            {
                item.SetValue(instance, container.Resolve(item.PropertyType));
            }
            return instance;
        }

        public T Resolve<T>()
        {
            return (T)Resolve(typeof(T));
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
