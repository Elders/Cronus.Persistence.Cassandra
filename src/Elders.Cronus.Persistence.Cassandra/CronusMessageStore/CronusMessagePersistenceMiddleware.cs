using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Middleware;
using Elders.Cronus.Persistence.Cassandra.CronusMessageStore;
using System;

namespace Elders.Cronus.Persistence.Cassandra.CronusMessageStore
{
    public class CronusMessagePersistenceMiddleware : Middleware<HandleContext>
    {
        readonly ICronusMessageStore store;

        public CronusMessagePersistenceMiddleware(ICronusMessageStore store)
        {
            this.store = store;
        }

        protected override void Run(Execution<HandleContext> execution)
        {
            store.Append(execution.Context.Message);
        }
    }

    public static class CronusMessagePersistenceMiddlewareConfig
    {
        /// <summary>
        /// Allows to store every message which was routed trough the system. <see cref="CronusMessage" /> is the object stored there.
        /// </summary>
        /// <param name="self">The current middleware</param>
        /// <param name="settings"></param>
        /// <returns></returns>
        public static Middleware<HandleContext> UseMessageStorage(this Middleware<HandleContext> self, Func<ICronusMessageStore> settings)
        {
            self.Use(new CronusMessagePersistenceMiddleware(settings()));
            return self;
        }
    }
}
