using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Rhino.ServiceBus.SqlQueues
{
    public class SqlQueue : ISqlQueue
    {
        private readonly string _queueName;
        private readonly string _connectionString;
        private readonly Uri _endpoint;
        
        public SqlQueue(string queueName,string connectionString,Uri endpoint)
        {
            _queueName = queueName;
            _connectionString = connectionString;
            _endpoint = endpoint;
        }

        public void MoveTo(string subQueue, Message message)
        {
            using (var command = SqlTransactionContext.Current.Connection.CreateCommand())
            {
                command.Transaction = SqlTransactionContext.Current.Transaction;
                command.CommandText = "Queue.MoveMessage";
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.AddWithValue("@Endpoint", _endpoint.ToString());
                command.Parameters.AddWithValue("@Queue", _queueName);
                command.Parameters.AddWithValue("@SubQueue", subQueue);
                command.Parameters.AddWithValue("@MessageId", message.Id);

                command.ExecuteNonQuery();
            }
        }

        public void EnqueueDirectlyTo(string subQueue, MessagePayload messagePayload)
        {
            using (var command = SqlTransactionContext.Current.Connection.CreateCommand())
            {
                command.CommandText = "Queue.EnqueueMessage";
                command.CommandType = CommandType.StoredProcedure;
                command.Transaction = SqlTransactionContext.Current.Transaction;
                command.Parameters.AddWithValue("@Endpoint", _endpoint.ToString());
                command.Parameters.AddWithValue("@Queue", _queueName);
                command.Parameters.AddWithValue("@SubQueue", subQueue);
                command.Parameters.AddWithValue("@Payload", messagePayload.Data);
                command.Parameters.AddWithValue("@Headers", MessagePayload.CompressHeaders(messagePayload.Headers));
                command.Parameters.AddWithValue("@ProcessingUntil", DateTime.UtcNow);
                command.Parameters.AddWithValue("@CreatedAt", messagePayload.SentAt);
                command.Parameters.AddWithValue("@ExpiresAt", DateTime.UtcNow.AddDays(2));

                command.ExecuteNonQuery();
            }
        }

        public string QueueName
        {
            get { return _queueName; }
        }

        public IEnumerable<Message> GetAllMessages(string queue)
        {
            var rawList = new List<RawMessage>();
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = "Queue.RecieveMessages";
                        command.CommandType = CommandType.StoredProcedure;
                        command.Transaction = transaction;
                        command.Parameters.AddWithValue("@Endpoint", _endpoint.ToString());
                        command.Parameters.AddWithValue("@Queue", _queueName);
                        command.Parameters.AddWithValue("@SubQueue", queue);

                        var reader = command.ExecuteReader();
                        var messageIdIndex = reader.GetOrdinal("MessageId");
                        var queueIdIndex = reader.GetOrdinal("QueueId");
                        var createdAtIndex = reader.GetOrdinal("CreatedAt");
                        var processingUntilIndex = reader.GetOrdinal("ProcessingUntil");
                        var expiresAtIndex = reader.GetOrdinal("ExpiresAt");
                        var processedIndex = reader.GetOrdinal("Processed");
                        var headersIndex = reader.GetOrdinal("Headers");
                        var payloadIndex = reader.GetOrdinal("Payload");
                        while (reader.Read())
                        {
                            var raw = new RawMessage
                                          {
                                              CreatedAt = reader.GetDateTime(createdAtIndex),
                                              ExpiresAt = reader.GetDateTime(expiresAtIndex),
                                              Headers = reader.GetString(headersIndex),
                                              MessageId = reader.GetInt32(messageIdIndex),
                                              Processed = reader.GetBoolean(processedIndex),
                                              ProcessingUntil = reader.GetDateTime(processingUntilIndex),
                                              QueueId = reader.GetInt32(queueIdIndex),
                                              QueueName = _queueName,
                                              SubQueueName = queue
                                          };
                            raw.Payload = reader.GetSqlBinary(payloadIndex).Value;
                            rawList.Add(raw);
                        }
                        reader.Close();
                    }
                    transaction.Commit();
                }
            }
            return rawList.Select(raw => raw.ToMessage());
        }

        public Message PeekById(int messageId)
        {
            RawMessage raw = null;

            using (var command = SqlTransactionContext.Current.Connection.CreateCommand())
            {
                command.CommandText = "Queue.PeekMessageById";
                command.CommandType = CommandType.StoredProcedure;
                command.Transaction = SqlTransactionContext.Current.Transaction;
                command.Parameters.AddWithValue("@MessageId", messageId);

                var reader = command.ExecuteReader();
                var messageIdIndex = reader.GetOrdinal("MessageId");
                var queueIdIndex = reader.GetOrdinal("QueueId");
                var createdAtIndex = reader.GetOrdinal("CreatedAt");
                var processingUntilIndex = reader.GetOrdinal("ProcessingUntil");
                var expiresAtIndex = reader.GetOrdinal("ExpiresAt");
                var processedIndex = reader.GetOrdinal("Processed");
                var headersIndex = reader.GetOrdinal("Headers");
                var payloadIndex = reader.GetOrdinal("Payload");
                var subQueueNameIndex = reader.GetOrdinal("SubQueueName");
                while (reader.Read())
                {
                    raw = new RawMessage
                              {
                                  CreatedAt = reader.GetDateTime(createdAtIndex),
                                  ExpiresAt = reader.GetDateTime(expiresAtIndex),
                                  Headers = reader.GetString(headersIndex),
                                  MessageId = reader.GetInt32(messageIdIndex),
                                  Processed = reader.GetBoolean(processedIndex),
                                  ProcessingUntil = reader.GetDateTime(processingUntilIndex),
                                  QueueId = reader.GetInt32(queueIdIndex),
                                  QueueName = _queueName,
                                  SubQueueName = reader.GetString(subQueueNameIndex)
                              };
                    raw.Payload = reader.GetSqlBinary(payloadIndex).Value;
                }
            }
            return raw == null ? null : raw.ToMessage();
        }

        public SqlTransactionContext BeginTransaction()
        {
            return new SqlTransactionContext(new SqlConnection(_connectionString));
        }
    }
}