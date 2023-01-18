namespace KafkaTools
{
    public class User
    {
        public User()
        {

        }

        public User(string username, int age)
        {
            Username = username;
            Age = age;
        }

        public string Username { get; set; }

        public int Age { get; set; }
    }
}
