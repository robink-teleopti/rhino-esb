using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using Rhino.ServiceBus.Util;

namespace Rhino.ServiceBus.SqlQueues
{
    public class SqlQueueManager : IDisposable
    {
        private readonly Uri _endpoint;
        private readonly string _connectionString;

        public SqlQueueManager(Uri endpoint, string connectionString)
        {
            _endpoint = endpoint;
            _connectionString = connectionString;
        }

        public void DisposeRudely()
        {
            
        }

        public void Dispose()
        {
            
        }

        public void CreateQueues(string queueName)
        {
            using (BeginTransaction())
            {
                using (var command = SqlTransactionContext.Current.Connection.CreateCommand())
                {
                    command.CommandText = "Queue.CreateQueueIfMissing";
                    command.CommandType = CommandType.StoredProcedure;
                    command.Transaction = SqlTransactionContext.Current.Transaction;
                    command.Parameters.AddWithValue("@Endpoint", _endpoint.ToString());
                    command.Parameters.AddWithValue("@Queue", queueName);

                    command.ExecuteNonQuery();
                }
                SqlTransactionContext.Current.Transaction.Commit();
            }
        }

        public ISqlQueue GetQueue(string queueName)
        {
            return new SqlQueue(queueName, _connectionString, _endpoint);
        }

        public void Peek(string queueName)
        {
            using (var command = SqlTransactionContext.Current.Connection.CreateCommand())
            {
                command.CommandText = "Queue.PeekMessage";
                command.CommandType = CommandType.StoredProcedure;
                command.Transaction = SqlTransactionContext.Current.Transaction;
                command.Parameters.AddWithValue("@Endpoint", _endpoint.ToString());
                command.Parameters.AddWithValue("@Queue", queueName);
                command.Parameters.AddWithValue("@SubQueue", DBNull.Value);

                var manualResetEvent = new ManualResetEvent(false);
                while (true)
                {
                    var reader = command.ExecuteReader();
                    if (reader.HasRows)
                    {
                        reader.Close();
                        return;
                    }
                    reader.Close();
                    manualResetEvent.WaitOne(TimeSpan.FromSeconds(2));
                }
            }
        }

        public Message Receive(string queueName, TimeSpan timeOut)
        {
            RawMessage raw = null;

            using (var command = SqlTransactionContext.Current.Connection.CreateCommand())
            {
                command.CommandTimeout = timeOut.Seconds;
                command.CommandText = "Queue.RecieveMessage";
                command.CommandType = CommandType.StoredProcedure;
                command.Transaction = SqlTransactionContext.Current.Transaction;
                command.Parameters.AddWithValue("@Endpoint", _endpoint.ToString());
                command.Parameters.AddWithValue("@Queue", queueName);
                command.Parameters.AddWithValue("@SubQueue", DBNull.Value);

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
                    raw = new RawMessage
                              {
                                  CreatedAt = reader.GetDateTime(createdAtIndex),
                                  Headers = reader.GetString(headersIndex),
                                  MessageId = reader.GetInt32(messageIdIndex),
                                  Processed = reader.GetBoolean(processedIndex),
                                  ProcessingUntil = reader.GetDateTime(processingUntilIndex),
                                  QueueId = reader.GetInt32(queueIdIndex),
                                  QueueName = queueName,
                                  SubQueueName = null
                              };
                    if (!reader.IsDBNull(expiresAtIndex))
                        raw.ExpiresAt = reader.GetDateTime(expiresAtIndex);

                    if (!reader.IsDBNull(payloadIndex))
                        raw.Payload = reader.GetSqlBinary(payloadIndex).Value;
                }
                reader.Close();
            }
            
            if (raw==null) throw new TimeoutException();
            return raw.ToMessage();
        }

        public void Send(Uri uri, MessagePayload payload)
        {
            SqlTransactionContext transactionToCommit = null;
            if (SqlTransactionContext.Current==null)
            {
                transactionToCommit = BeginTransaction();
            }
                using (var command = SqlTransactionContext.Current.Connection.CreateCommand())
                {
                    command.CommandText = "Queue.EnqueueMessage";
                    command.CommandType = CommandType.StoredProcedure;
                    command.Transaction = SqlTransactionContext.Current.Transaction;
                    command.Parameters.AddWithValue("@Endpoint", uri.ToString());
                    command.Parameters.AddWithValue("@Queue", uri.GetQueueName());
                    command.Parameters.AddWithValue("@SubQueue", DBNull.Value);

                    var contents = new RawMessage
                                       {
                                           CreatedAt = payload.SentAt,
                                           Payload = payload.Data,
                                           ProcessingUntil = payload.SentAt
                                       };
                    contents.SetHeaders(payload.Headers);

                    command.Parameters.AddWithValue("@CreatedAt", contents.CreatedAt);
                    command.Parameters.AddWithValue("@Payload", contents.Payload);
                    command.Parameters.AddWithValue("@ExpiresAt", DBNull.Value);
                    command.Parameters.AddWithValue("@ProcessingUntil", contents.CreatedAt);
                    command.Parameters.AddWithValue("@Headers", contents.Headers);

                    command.ExecuteNonQuery();
                }
            if (transactionToCommit!=null)
            {
                transactionToCommit.Transaction.Commit();
                transactionToCommit.Dispose();
            }
        }

        public SqlTransactionContext BeginTransaction()
        {
            return new SqlTransactionContext(new SqlConnection(_connectionString));
        }
    }

    public class SqlTransactionContext : IDisposable
    {
        private readonly SqlConnection _connection;
        private readonly SqlTransaction _transaction;

        public SqlTransactionContext(SqlConnection connection)
        {
            if (Current!=null)
            {
                throw new InvalidOperationException("Just one open context per thread is allowed!");
            }
            _connection = connection;
            _connection.Open();
            _transaction = connection.BeginTransaction();
            Current = this;
        }

        [ThreadStatic]
        public static SqlTransactionContext Current;

        public SqlConnection Connection { get { return _connection; } }
        public SqlTransaction Transaction { get { return _transaction; } }

        public void Dispose()
        {
            Current = null;
            _transaction.Dispose();
            _connection.Dispose();
        }
    }
}