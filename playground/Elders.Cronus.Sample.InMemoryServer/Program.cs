namespace Elders.Cronus.Sample.InMemoryServer
{
    class Program
    {
        public static void Main(string[] args)
        {
            //log4net.Config.XmlConfigurator.Configure();
            //var sf = BuildNHibernateSessionFactory();
            //const string BC = "ContextlessDomain";//This can be fixed in futre versions.I just don't have time now. Log issue probably? Answer: Yes please.
            //var cfg = new CronusSettings()
            //    .UseContractsFromAssemblies(new Assembly[] { Assembly.GetAssembly(typeof(RegisterAccount)), Assembly.GetAssembly(typeof(CreateUser)) })
            //    .UseCassandraEventStore(eventStore => eventStore
            //        .SetConnectionStringName("cronus_es")
            //        .SetAggregateStatesAssembly(typeof(AccountState))
            //        .WithNewStorageIfNotExists());

            //var configurationInstance = cfg.WithDefaultPublishersInMemory(BC, new Assembly[] { typeof(AccountAppService).Assembly, typeof(UserAppService).Assembly, typeof(UserProjection).Assembly }, (type, context) =>
            //         {
            //             return FastActivator.CreateInstance(type)
            //                 .AssignPropertySafely<IAggregateRootApplicationService>(x => x.Repository = context.BatchContext.Get<Lazy<IAggregateRepository>>().Value)
            //                 .AssignPropertySafely<IPort>(x => x.CommandPublisher = (cfg as IHaveCommandPublisher).CommandPublisher.Value)
            //                 .AssignPropertySafely<IHaveNhibernateSession>(x => x.Session = context.BatchContext.Get<Lazy<ISession>>().Value);
            //         },
            //         new UnitOfWorkFactory() { CreateBatchUnitOfWork = () => new BatchScope(sf) }
            //         ).GetInstance();

            //while (true)
            //{
            //    int counter = 0;
            //    while (true)
            //    {
            //        configurationInstance.CommandPublisher.Publish(new RegisterAccount(new AccountId(Guid.NewGuid()), "awwwww@email.com"));
            //        counter++;
            //        if (counter % 200 == 0)
            //        {
            //            Console.WriteLine(counter);
            //        }

            //    }

            //    //Console.WriteLine("Sent");
            //    //Console.ReadLine();
            //}
        }


        //static ISessionFactory BuildNHibernateSessionFactory()
        //{
        //    var typesThatShouldBeMapped = Assembly.GetAssembly(typeof(UserProjection)).GetExportedTypes().Where(t => t.Namespace.EndsWith("DTOs"));
        //    var cfg = new NHibernate.Cfg.Configuration()
        //        .AddAutoMappings(typesThatShouldBeMapped)
        //        .Configure()
        //        .CreateDatabase();

        //    return cfg.BuildSessionFactory();
        //}
    }
}