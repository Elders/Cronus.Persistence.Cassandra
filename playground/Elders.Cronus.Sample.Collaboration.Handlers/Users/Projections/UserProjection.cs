using Elders.Cronus.Projections;
using Elders.Cronus.Sample.Collaboration.Users.Events;
using System.Runtime.Serialization;
using System;

namespace Elders.Cronus.Sample.Collaboration.Users.Projections
{
    [DataContract(Name = "52f8d0a8-90b2-4def-b029-a0901d0145db")]
    public class UserProjection : ProjectionDefinition<UserItem, UserId>,
        IEventHandler<UserCreated>
    {
        public UserProjection()
        {
            Subscribe<UserCreated>(x => x.Id);
        }

        public void Handle(UserCreated message)
        {
        }

        public void Handle(UserCreated1 @event)
        {
            // throw new NotImplementedException();
        }
    }

    //[DataContract(Name = "e588e9ee-ef50-4e02-ac83-189adca51a6c")]
    //public class UserProjection1 : IProjection, IEventHandler<UserCreated>
    //{
    //    static int counter = 0;
    //    static DateTime last = DateTime.UtcNow;

    //    public UserProjection1()
    //    {
    //        
    //    }

    //    public void Handle(UserCreated message)
    //    {
    //        ++counter;
    //        if ((DateTime.UtcNow - last).TotalSeconds > 1)
    //        {
    //            last = DateTime.UtcNow;
    //            Console.WriteLine(counter);
    //            counter = 0;
    //        }
    //    }
    //}

    [DataContract(Name = "610c3c22-c10b-4d5a-9ba5-0426cf275d18")]
    public class UserItem
    {
        [DataMember(Order = 1)]
        public UserId Id { get; set; }
    }
}
