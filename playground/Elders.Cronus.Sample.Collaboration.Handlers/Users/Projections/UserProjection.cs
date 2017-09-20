using Elders.Cronus.DomainModeling;
using Elders.Cronus.DomainModeling.Projections;
using Elders.Cronus.Sample.Collaboration.Users.DTOs;
using Elders.Cronus.Sample.Collaboration.Users.Events;
using System.Runtime.Serialization;

namespace Elders.Cronus.Sample.Collaboration.Users.Projections
{
    [DataContract(Name = "e588e9ee-ef50-4e02-ac83-189adca51a6c")]
    public class UserProjection : IProjection, IEventHandler<UserCreated>
    {
        public void Handle(UserCreated message)
        {
            var usr = new User();
            usr.Id = message.Id.Id;
            usr.Email = message.Email;
            // Session.Save(usr);
        }
    }

    [DataContract(Name = "610c3c22-c10b-4d5a-9ba5-0426cf275d18")]
    public class UserItem
    {
        [DataMember(Order = 1)]
        public UserId Id { get; set; }
    }
}
