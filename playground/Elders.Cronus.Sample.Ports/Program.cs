using System;
using System.Reflection;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.IocContainer;
using Elders.Cronus.Pipeline;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Pipeline.Hosts;
using Elders.Cronus.Pipeline.Transport;
using Elders.Cronus.Pipeline.Transport.RabbitMQ.Config;
using Elders.Cronus.Sample.Collaboration.Users.Commands;
using Elders.Cronus.Sample.Collaboration.Users.Projections;
using Elders.Cronus.Sample.IdentityAndAccess.Accounts.Commands;
using Elders.Cronus.Serializer;
using Elders.Cronus.Transport.AzureServiceBus.Config;

namespace Elders.Cronus.Sample.Ports
{
    class Program
    {
        static CronusHost host;
        public static void Main(string[] args)
        {
            //Thread.Sleep(9000);
            log4net.Config.XmlConfigurator.Configure();

            var container = new Container();

            Func<IPipelineTransport> transport = () => container.Resolve<IPipelineTransport>();
            Func<ISerializer> serializer = () => container.Resolve<ISerializer>();

            container.RegisterSingleton<IPublisher<ICommand>>(() => new PipelinePublisher<ICommand>(transport(), serializer()));

            var COLL_POOOOORTHandlerFactory = new PortHandlerFactory(container, null);
            var cfg = new CronusSettings(container)
                .UseContractsFromAssemblies(new Assembly[]
                {
                    Assembly.GetAssembly(typeof(RegisterAccount)),
                    Assembly.GetAssembly(typeof(CreateUser))
                })
                .UseRabbitMqTransport(x => x.Server = "10.0.2.4")
                //.UseAzureServiceBusTransport(x => x.ConnectionString = "Endpoint=sb://mvclientshared-servicebus-test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YStt1qtFInb3kp2oIj76c6ibEzlSH4oPOSjAXBkY74g=")
                .UsePortConsumer(consumable => consumable
                    .WithDefaultPublishers()
                    .UseRabbitMqTransport(x => x.Server = "10.0.2.4")
                    //.UseAzureServiceBusTransport(x => x.ConnectionString = "Endpoint=sb://mvclientshared-servicebus-test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YStt1qtFInb3kp2oIj76c6ibEzlSH4oPOSjAXBkY74g=")
                    .SetNumberOfConsumerThreads(1)
                    .UsePorts(c => c.RegisterHandlersInAssembly(new[] { Assembly.GetAssembly(typeof(UserProjection)) }, COLL_POOOOORTHandlerFactory.Create)));

            (cfg as ISettingsBuilder).Build();

            host = container.Resolve<CronusHost>();
            host.Start();

            Console.WriteLine("Ports started");
            Console.ReadLine();

            host.Stop();
        }

        public class PortHandlerFactory
        {
            private readonly IContainer container;
            private readonly string namedInstance;

            public PortHandlerFactory(IContainer container, string namedInstance)
            {
                this.container = container;
                this.namedInstance = namedInstance;
            }

            public object Create(Type handlerType)
            {
                var handler = FastActivator
                    .CreateInstance(handlerType)
                    .AssignPropertySafely<IPort>(x => x.CommandPublisher = container.Resolve<IPublisher<ICommand>>(namedInstance));
                return handler;
            }
        }
    }
}
