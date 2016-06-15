namespace Elders.Cronus.Sample.Player
{

    class Program
    {
        static void Main(string[] args)
        {
            //var sf = BuildSessionFactory();

            //var cfg = new CronusSettings()
            //    .UseContractsFromAssemblies(new Assembly[] { Assembly.GetAssembly(typeof(UserState)), Assembly.GetAssembly(typeof(UserCreated)) })
            //    .WithDefaultPublishersInMemory("Collaboration", new Assembly[] { Assembly.GetAssembly(typeof(UserProjection)) }, (type, context) =>
            //                {
            //                    return FastActivator.CreateInstance(type)
            //                        .AssignPropertySafely<IHaveNhibernateSession>(x => x.Session = context.HandlerContext.Get<Lazy<ISession>>().Value);
            //                },
            //                new UnitOfWorkFactory() { CreateHandlerUnitOfWork = () => new HandlerScope(sf) });

            //cfg.UseCassandraEventStore(eventStore => eventStore
            //        .SetConnectionStringName("cronus_es")
            //        .SetAggregateStatesAssembly(typeof(UserState)));



            //new CustomCronusPlayer(cfg.GetInstance()).Replay();
        }

        //static ISessionFactory BuildSessionFactory()
        //{
        //    var typesThatShouldBeMapped = Assembly.GetAssembly(typeof(UserProjection)).GetExportedTypes().Where(t => t.Namespace.EndsWith("DTOs"));
        //    var cfg = new NHibernate.Cfg.Configuration();
        //    cfg = cfg.AddAutoMappings(typesThatShouldBeMapped);
        //    cfg.Configure();
        //    cfg.CreateDatabase_AND_OVERWRITE_EXISTING_DATABASE();
        //    return cfg.BuildSessionFactory();
        //}


    }

    //public class CustomCronusPlayer
    //{
    //    static readonly log4net.ILog log = log4net.LogManager.GetLogger(typeof(CronusPlayer));

    //    private readonly CronusConfiguration configuration;

    //    public CustomCronusPlayer(CronusConfiguration configuration)
    //    {
    //        this.configuration = configuration;
    //    }

    //    public void Replay()
    //    {
    //        Console.WriteLine("Start replaying events...");

    //        var publisher = configuration.EventPublisher;
    //        int totalMessagesPublished = 0;
    //        foreach (var evnt in configuration.EventStores.Single().Value.Player.GetEventsFromStart())
    //        {
    //            totalMessagesPublished++;
    //            configuration.EventPublisher.Publish(evnt);
    //        }
    //        Console.WriteLine("Replay finished.");
    //    }

    //}

}