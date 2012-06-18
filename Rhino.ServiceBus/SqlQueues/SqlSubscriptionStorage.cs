using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using log4net;
using Rhino.ServiceBus.DataStructures;
using Rhino.ServiceBus.Exceptions;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.MessageModules;
using Rhino.ServiceBus.Messages;
using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.SqlQueues
{
    public class SqlSubscriptionStorage : ISubscriptionStorage, IDisposable, IMessageModule
    {
        private const string SubscriptionsKey = "subscriptions_";

        private readonly Hashtable<string, List<WeakReference>> localInstanceSubscriptions =
            new Hashtable<string, List<WeakReference>>();

        private readonly ILog logger = LogManager.GetLogger(typeof (SqlSubscriptionStorage));

        private readonly IMessageSerializer messageSerializer;
        private readonly IReflection reflection;

        private readonly MultiValueIndexHashtable<Guid, string, Uri, int> remoteInstanceSubscriptions =
            new MultiValueIndexHashtable<Guid, string, Uri, int>();

        private readonly Hashtable<TypeAndUriKey, IList<int>> subscriptionMessageIds =
            new Hashtable<TypeAndUriKey, IList<int>>();

        private readonly string connectionString;
        private readonly Hashtable<string, HashSet<Uri>> subscriptions = new Hashtable<string, HashSet<Uri>>();

    	private bool currentlyLoadingPersistentData;
        private string localEndpoint;

        public SqlSubscriptionStorage(
            string connectionString, 
            string localEndpoint,
            IMessageSerializer messageSerializer,
            IReflection reflection)
        {
            this.connectionString = connectionString;
            this.localEndpoint = localEndpoint;
            this.messageSerializer = messageSerializer;
            this.reflection = reflection;
        }

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion

        #region IMessageModule Members

		void IMessageModule.Init(ITransport transport, IServiceBus bus)
        {
            transport.AdministrativeMessageArrived += HandleAdministrativeMessage;
        }

		void IMessageModule.Stop(ITransport transport, IServiceBus bus)
        {
            transport.AdministrativeMessageArrived -= HandleAdministrativeMessage;
        }

        #endregion

        #region ISubscriptionStorage Members

        public void Initialize()
        {
            var messages = new List<CurrentMessageInformation>();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                {
                    var command = connection.CreateCommand();
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = "Queue.GetItemsByKey";
                    command.Transaction = transaction;
                    command.Parameters.AddWithValue("@Key", SubscriptionsKey+localEndpoint);
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var value = reader.GetSqlBinary(reader.GetOrdinal("Value"));

                        object[] msgs;
                        try
                        {
                            msgs = messageSerializer.Deserialize(new MemoryStream(value.Value));
                        }
                        catch (Exception e)
                        {
                            throw new SubscriptionException("Could not deserialize message from subscription queue", e);
                        }

                        try
                        {
                            currentlyLoadingPersistentData = true;
                            foreach (var msg in msgs)
                            {
                                messages.Add(new CurrentMessageInformation
                                                                {
                                                                    AllMessages = msgs,
                                                                    Message = msg,
                                                                });
                            }
                        }
                        catch (Exception e)
                        {
                            throw new SubscriptionException("Failed to process subscription records", e);
                        }
                    }
                    reader.Close();
                    transaction.Commit();
                }
            }
            foreach (var currentMessageInformation in messages)
            {
                HandleAdministrativeMessage(currentMessageInformation);
            }
            currentlyLoadingPersistentData = false;
        }

        public IEnumerable<Uri> GetSubscriptionsFor(Type type)
        {
            HashSet<Uri> subscriptionForType = null;
            subscriptions.Read(reader => reader.TryGetValue(type.FullName, out subscriptionForType));
            var subscriptionsFor = subscriptionForType ?? new HashSet<Uri>();

            List<Uri> instanceSubscriptions;
            remoteInstanceSubscriptions.TryGet(type.FullName, out instanceSubscriptions);

            subscriptionsFor.UnionWith(instanceSubscriptions);

            return subscriptionsFor;
        }

        public void RemoveLocalInstanceSubscription(IMessageConsumer consumer)
        {
            var messagesConsumes = reflection.GetMessagesConsumed(consumer);
            var changed = false;
            var list = new List<WeakReference>();

            localInstanceSubscriptions.Write(writer =>
            {
                foreach (var type in messagesConsumes)
                {
                    List<WeakReference> value;

                    if (writer.TryGetValue(type.FullName, out value) == false)
                        continue;
                    writer.Remove(type.FullName);
                    list.AddRange(value);
                }
            });

            foreach (var reference in list)
            {
                if (ReferenceEquals(reference.Target, consumer))
                    continue;

                changed = true;
            }
            if (changed)
                RaiseSubscriptionChanged();
        }

        public object[] GetInstanceSubscriptions(Type type)
        {
            List<WeakReference> value = null;

            localInstanceSubscriptions.Read(reader => reader.TryGetValue(type.FullName, out value));

            if (value == null)
                return new object[0];

            var liveInstances = value
                .Select(x => x.Target)
                .Where(x => x != null)
                .ToArray();

            if (liveInstances.Length != value.Count) //cleanup
            {
                localInstanceSubscriptions.Write(writer => value.RemoveAll(x => x.IsAlive == false));
            }

            return liveInstances;
        }

        public event Action SubscriptionChanged;

        public bool AddSubscription(string type, string endpoint)
        {
            var added = false;
            subscriptions.Write(writer =>
            {
                HashSet<Uri> subscriptionsForType;
                if (writer.TryGetValue(type, out subscriptionsForType) == false)
                {
                    subscriptionsForType = new HashSet<Uri>();
                    writer.Add(type, subscriptionsForType);
                }

                var uri = new Uri(endpoint);
                added = subscriptionsForType.Add(uri);
                logger.InfoFormat("Added subscription for {0} on {1}",
                                  type, uri);
            });

            RaiseSubscriptionChanged();
            return added;
        }

        public void RemoveSubscription(string type, string endpoint)
        {
            var uri = new Uri(endpoint);
            RemoveSubscriptionMessageFromPht(type, uri);

            subscriptions.Write(writer =>
            {
                HashSet<Uri> subscriptionsForType;

                if (writer.TryGetValue(type, out subscriptionsForType) == false)
                {
                    subscriptionsForType = new HashSet<Uri>();
                    writer.Add(type, subscriptionsForType);
                }

                subscriptionsForType.Remove(uri);

                logger.InfoFormat("Removed subscription for {0} on {1}",
                                  type, endpoint);
            });

            RaiseSubscriptionChanged();
        }

        public void AddLocalInstanceSubscription(IMessageConsumer consumer)
        {
            localInstanceSubscriptions.Write(writer =>
            {
                foreach (var type in reflection.GetMessagesConsumed(consumer))
                {
                    List<WeakReference> value;
                    if (writer.TryGetValue(type.FullName, out value) == false)
                    {
                        value = new List<WeakReference>();
                        writer.Add(type.FullName, value);
                    }
                    value.Add(new WeakReference(consumer));
                }
            });
            RaiseSubscriptionChanged();
        }

        #endregion

        private void AddMessageIdentifierForTracking(int messageId, string messageType, Uri uri)
        {
            subscriptionMessageIds.Write(writer =>
            {
                var key = new TypeAndUriKey {TypeName = messageType, Uri = uri};
                IList<int> value;
                if (writer.TryGetValue(key, out value) == false)
                {
                    value = new List<int>();
                    writer.Add(key, value);
                }

                value.Add(messageId);
            });
        }

        private void RemoveSubscriptionMessageFromPht(string type, Uri uri)
        {
            subscriptionMessageIds.Write(writer =>
            {
                var key = new TypeAndUriKey
                {
                    TypeName = type,
                    Uri = uri
                };
                IList<int> messageIds;
                if (writer.TryGetValue(key, out messageIds) == false)
                    return;

                using (var connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (var transaction = connection.BeginTransaction())
                    {
                        using (var command = connection.CreateCommand())
                        {
                            command.Transaction = transaction;
                            command.CommandText = "Queue.RemoveItem";
                            command.CommandType = CommandType.StoredProcedure;
                            command.Parameters.AddWithValue("@Key", SubscriptionsKey+localEndpoint);
                            foreach (var msgId in messageIds)
                            {
                                command.Parameters.RemoveAt("@Id");
                                command.Parameters.AddWithValue("@Id", msgId);
                                command.ExecuteNonQuery();
                            }
                        }
                        transaction.Commit();
                    }
                }

                writer.Remove(key);
            });
        }

        public bool HandleAdministrativeMessage(CurrentMessageInformation msgInfo)
        {
            var addSubscription = msgInfo.Message as AddSubscription;
            if (addSubscription != null)
            {
                return ConsumeAddSubscription(addSubscription);
            }
            var removeSubscription = msgInfo.Message as RemoveSubscription;
            if (removeSubscription != null)
            {
                return ConsumeRemoveSubscription(removeSubscription);
            }
            var addInstanceSubscription = msgInfo.Message as AddInstanceSubscription;
            if (addInstanceSubscription != null)
            {
                return ConsumeAddInstanceSubscription(addInstanceSubscription);
            }
            var removeInstanceSubscription = msgInfo.Message as RemoveInstanceSubscription;
            if (removeInstanceSubscription != null)
            {
                return ConsumeRemoveInstanceSubscription(removeInstanceSubscription);
            }
            return false;
        }

        public bool ConsumeRemoveInstanceSubscription(RemoveInstanceSubscription subscription)
        {
            int msgId;
            if (remoteInstanceSubscriptions.TryRemove(subscription.InstanceSubscriptionKey, out msgId))
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (var transaction = connection.BeginTransaction())
                    {
                        using (var command = connection.CreateCommand())
                        {
                            command.CommandText = "Queue.RemoveItem";
                            command.CommandType = CommandType.StoredProcedure;
                            command.Transaction = transaction;
                            command.Parameters.AddWithValue("@Key", SubscriptionsKey+localEndpoint);
                            command.Parameters.AddWithValue("@Id", msgId);
                            command.ExecuteNonQuery();
                        }
                        transaction.Commit();
                    }
                }

                RaiseSubscriptionChanged();
            }
            return true;
        }

        public bool ConsumeAddInstanceSubscription(AddInstanceSubscription subscription)
        {
            int itemId;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                {
                    using (var command = connection.CreateCommand())
                    {
                        var message = new MemoryStream();
                        messageSerializer.Serialize(new[] { subscription }, message);
                        
                        command.CommandText = "Queue.AddItem";
                        command.CommandType = CommandType.StoredProcedure;
                        command.Transaction = transaction;
                        command.Parameters.AddWithValue("@Key", SubscriptionsKey+localEndpoint);
                        command.Parameters.AddWithValue("@Value", message.ToArray());
                        
                        itemId = (int)(decimal)command.ExecuteScalar();
                    }
                    transaction.Commit();
                }
            }

            remoteInstanceSubscriptions.Add(
                subscription.InstanceSubscriptionKey,
                subscription.Type,
                new Uri(subscription.Endpoint),
                itemId);

            RaiseSubscriptionChanged();
            return true;
        }

        public bool ConsumeRemoveSubscription(RemoveSubscription removeSubscription)
        {
            RemoveSubscription(removeSubscription.Type, removeSubscription.Endpoint.Uri.ToString());
            return true;
        }

        public bool ConsumeAddSubscription(AddSubscription addSubscription)
        {
            var newSubscription = AddSubscription(addSubscription.Type, addSubscription.Endpoint.Uri.ToString());

			if (newSubscription && currentlyLoadingPersistentData == false)
            {
                int itemId;
                using (var connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (var transaction = connection.BeginTransaction())
                    {
                        using (var command = connection.CreateCommand())
                        {
                            var stream = new MemoryStream();
                            messageSerializer.Serialize(new[] { addSubscription }, stream);

                            command.CommandText = "Queue.AddItem";
                            command.CommandType = CommandType.StoredProcedure;
                            command.Transaction = transaction;
                            command.Parameters.AddWithValue("@Key", SubscriptionsKey+localEndpoint);
                            command.Parameters.AddWithValue("@Value", stream.ToArray());

                            itemId = (int)(decimal)command.ExecuteScalar();
                        }
                        transaction.Commit();
                    }
                }

                AddMessageIdentifierForTracking(
                    itemId,
                    addSubscription.Type,
                    addSubscription.Endpoint.Uri);

                return true;
            }

            return false;
        }

        private void RaiseSubscriptionChanged()
        {
            var copy = SubscriptionChanged;
            if (copy != null)
                copy();
        }
    }

    public class AddItemRequest
    {
        public string Key { get; set; }

        public byte[] Data { get; set; }
    }

    public class GetItemsRequest
    {
        public string Key { get; set; }
    }

    public class RemoveItemRequest
    {
        public int Id { get; set; }

        public string Key { get; set; }
    }
}