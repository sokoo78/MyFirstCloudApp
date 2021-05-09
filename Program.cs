using System;
using System.IO;
using System.Threading.Tasks;

namespace MyFirstCloudApp
{
    class Program
    {
        // GCP API authentication data
        private const string Key = "mysharpproject-70dae710f400.json";
        private const string EnvironmentVariable = "GOOGLE_APPLICATION_CREDENTIALS";
        private const string ProjectId = "mysharpproject";

        static async Task Main(string[] args)
        {
            // Set up environment variable for GCP
            Environment.SetEnvironmentVariable(EnvironmentVariable,
                Path.Combine(AppDomain.CurrentDomain.BaseDirectory, Key), EnvironmentVariableTarget.Process);

            Console.WriteLine("Hello Cloud!\n");
            Console.ReadKey();

            // Test the API
            Console.WriteLine("Create new topic..\n");
            var topic = await PubSub.CreateTopic(ProjectId, "MyTopicFromCsharp");
            Console.ReadKey();

            Console.WriteLine("Subscribe to the new topic..\n");
            var subscription = await PubSub.CreateSubscription(ProjectId, topic.TopicName, "MySubscriptionFromCsharp");
            Console.ReadKey();

            Console.WriteLine("Publish a text message to the topic..\n");
            var messageId = await PubSub.Publish(topic.TopicName, "My First Message From C# App :)");
            Console.WriteLine($" => Message ID: {messageId}\n");
            Console.ReadKey();

            Console.WriteLine("Pull the text message from the topic..\n");
            var messages = await PubSub.Pull(subscription.SubscriptionName);
            foreach (var message in messages)
            {
                Console.WriteLine($" => Received message {message.MessageId} published at {message.PublishTime.ToDateTime()}");
                Console.WriteLine($" => Text: '{message.Data.ToStringUtf8()}'");
            }
            Console.ReadKey();

            Console.WriteLine("Delete subscribtion..\n");
            await PubSub.DeleteSubscription(subscription.SubscriptionName);
            Console.ReadKey();

            Console.WriteLine("Delete topic..\n");
            await PubSub.DeleteTopic(topic.TopicName);
            Console.ReadKey();
        }
    }
}
