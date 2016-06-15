using Elders.Cronus.DomainModeling;
using Elders.Cronus.Sample.Collaboration.Users.Events;

namespace Elders.Cronus.Sample.Collaboration.Users
{
    public sealed class User : AggregateRoot<UserState>
    {
        User() { }

        public User(UserId collaboratorId, string email)
        {
            var evnt = new UserCreated(collaboratorId, email);
            Apply(evnt);
        }

        public void Rename(string firstName, string lastName)
        {
            var evnt = new UserRenamed(state.Id, firstName, lastName);
            Apply(evnt);
        }
    }
}
