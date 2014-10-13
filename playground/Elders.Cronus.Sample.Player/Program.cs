using System;
using System.Linq;
using System.Reflection;
using Elders.Cronus.UnitOfWork;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Pipeline.Hosts;
using Elders.Cronus.Pipeline.Transport.InMemory.Config;
using Elders.Cronus.Sample.Collaboration;
using Elders.Cronus.Sample.Collaboration.Users;
using Elders.Cronus.Sample.Collaboration.Users.Events;
using Elders.Cronus.Sample.Collaboration.Users.Projections;
using Elders.Cronus.Sample.CommonFiles;
using NHibernate;
using Elders.Cronus.Persistence.Cassandra.Config;

namespace Elders.Cronus.Sample.Player
{
    public class HandlerScope : IHandlerUnitOfWork
    {
        private readonly ISessionFactory sessionFactory;
        private ISession session;
        private ITransaction transaction;

        public HandlerScope(ISessionFactory sessionFactory)
        {
            this.sessionFactory = sessionFactory;
        }

        public void Begin()
        {
            Context = new UnitOfWorkContext();

            Lazy<ISession> lazySession = new Lazy<ISession>(() =>
            {
                session = sessionFactory.OpenSession();
                transaction = session.BeginTransaction();
                return session;
            });
            Context.Set<Lazy<ISession>>(lazySession);
        }

        public void End()
        {
            if (session != null)
            {
                transaction.Commit();
                session.Clear();
                session.Close();
            }
        }

        public IUnitOfWorkContext Context { get; set; }
    }

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

        static ISessionFactory BuildSessionFactory()
        {
            var typesThatShouldBeMapped = Assembly.GetAssembly(typeof(UserProjection)).GetExportedTypes().Where(t => t.Namespace.EndsWith("DTOs"));
            var cfg = new NHibernate.Cfg.Configuration();
            cfg = cfg.AddAutoMappings(typesThatShouldBeMapped);
            cfg.Configure();
            cfg.CreateDatabase_AND_OVERWRITE_EXISTING_DATABASE();
            return cfg.BuildSessionFactory();
        }


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