using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Cloud.PubSub.V1;
using Grpc.Core;

namespace MyFirstCloudApp
{
    public static class PubSub
    {
        public static async Task<Topic> CreateTopic(string projectId, string topicId)
        {
            var publisher = await PublisherServiceApiClient.CreateAsync();
            var topicName = TopicName.FromProjectTopic(projectId, topicId);
            Topic topic = null;
            try
            {
                topic = await publisher.CreateTopicAsync(topicName);
                Console.WriteLine($" => Topic {topic.Name} created.");
                return topic;
            }
            catch (RpcException e) when (e.Status.StatusCode == StatusCode.AlreadyExists)
            {
                Console.WriteLine($" => Topic {topic.Name} already exists.");
            }
            return null;
        }

        public static async Task<Subscription> CreateSubscription(string projectId, TopicName topicName, string subscriptionId)
        {
            var subscriberService = await SubscriberServiceApiClient.CreateAsync();
            var subscriptionName = new SubscriptionName(projectId, subscriptionId);
            try
            {
                var subscription = await subscriberService.CreateSubscriptionAsync(subscriptionName, topicName, pushConfig: null, ackDeadlineSeconds: 60);
                Console.WriteLine($" => Subscription {subscriptionId} created.");
                return subscription;
            }
            catch (RpcException e) when (e.Status.StatusCode == StatusCode.AlreadyExists)
            {
                Console.WriteLine($" => Subscription {subscriptionId} already exists.");
            }
            return null;
        }

        public static async Task<string> Publish(TopicName topicName, string message)
        {
            var publisher = await PublisherClient.CreateAsync(topicName);
            string messageId = null;
            try
            {
                messageId = await publisher.PublishAsync(message);
                Console.WriteLine($" => Publishing succeed. (Message: ({messageId}){message}");
            }
            catch (Exception e)
            {
                Console.WriteLine($" => Publishing failed.");
            }
            await publisher.ShutdownAsync(TimeSpan.FromSeconds(15));
            return messageId;
        }

        public static async Task<List<PubsubMessage>> Pull(SubscriptionName subscriptionName)
        {
            var subscriber = await SubscriberClient.CreateAsync(subscriptionName);
            var receivedMessages = new List<PubsubMessage>();
            // Start the subscriber listening for messages.
            await subscriber.StartAsync((msg, cancellationToken) =>
            {
                receivedMessages.Add(msg);                
                // Stop this subscriber after one message is received.
                subscriber.StopAsync(TimeSpan.FromSeconds(15));
                // Return Reply.Ack to indicate this message has been handled.
                return Task.FromResult(SubscriberClient.Reply.Ack);
            });
            return receivedMessages;
        }

        public static async Task<bool> DeleteTopic(TopicName topicName)
        {
            PublisherServiceApiClient publisherService = await PublisherServiceApiClient.CreateAsync();
            try
            {
                await publisherService.DeleteTopicAsync(topicName);
                Console.WriteLine($" => Topic deleted: {topicName}");
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine($" => Failed to delete {topicName}. (Error: {e.Message}");
                return false;
            }            
        }

        public static async Task<bool> DeleteSubscription(SubscriptionName subscriptionName)
        {
            var subscriberService = await SubscriberServiceApiClient.CreateAsync();            
            try
            {
                await subscriberService.DeleteSubscriptionAsync(subscriptionName);
                Console.WriteLine($" => Subscription deleted: {subscriptionName}");
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine($" => Failed to delete {subscriptionName}. (Error: {e.Message}");
                return false;
            }
        }
    }
}
