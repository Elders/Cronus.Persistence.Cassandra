using System;
using Elders.Cronus;
using Elders.Cronus.Sample.Collaboration.Users.Commands;
using Elders.Cronus.Sample.IdentityAndAccess.Accounts.Events;

namespace Elders.Cronus.Sample.Collaboration.Users.Ports
{
    public class UserPort : IPort,
        IEventHandler<AccountRegistered>
    {
        static int counter = 0;
        static DateTime last = DateTime.UtcNow;

        public IPublisher<ICommand> CommandPublisher { get; set; }

        public void Handle(AccountRegistered message)
        {
            UserId userId = new UserId(message.Id.Id);
            var email = message.Email;
            CommandPublisher.Publish(new CreateUser(userId, email));

            ++counter;
            if ((DateTime.UtcNow - last).TotalSeconds > 1)
            {
                last = DateTime.UtcNow;
                Console.WriteLine(counter);
                counter = 0;
            }
        }
    }
}
