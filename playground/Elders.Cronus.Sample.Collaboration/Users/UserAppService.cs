using Elders.Cronus.DomainModeling;
using Elders.Cronus.Sample.Collaboration.Users.Commands;

namespace Elders.Cronus.Sample.Collaboration.Users
{
    public class UserAppService : AggregateRootApplicationService<User>,
        ICommandHandler<CreateUser>,
        ICommandHandler<RenameUser>
    {

        static int counter = 0;
        static System.DateTime last = System.DateTime.UtcNow;

        public void Handle(RenameUser command)
        {
            Update(command.Id, user => user.Rename(command.FirstName, command.LastName));
        }

        public void Handle(CreateUser command)
        {
            Repository.Save(new User(command.Id, command.Email));

            ++counter;
            if ((System.DateTime.UtcNow - last).TotalSeconds > 1)
            {
                last = System.DateTime.UtcNow;
                System.Console.WriteLine(counter);
                counter = 0;
            }
        }
    }
}
