using System;
using System.Collections.Specialized;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Xml;
using log4net;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.Transport;
using Rhino.ServiceBus.Util;

namespace Rhino.ServiceBus.SqlQueues
{
    [CLSCompliant(false)]
    public class SqlQueuesTransport : ITransport
    {
        private readonly Uri endpoint;
        private readonly IEndpointRouter endpointRouter;
        private readonly IMessageSerializer messageSerializer;
        private readonly int threadCount;
        private readonly string connectionString;
        private SqlQueueManager _sqlQueueManager;
        private readonly Thread[] threads;
        private readonly string queueName;
        private volatile bool shouldContinue;
        private bool haveStarted;
        private readonly int numberOfRetries;
        private readonly IMessageBuilder<MessagePayload> messageBuilder;

        [ThreadStatic]
        private static SqlQueueCurrentMessageInformation currentMessageInformation;

        private readonly ILog logger = LogManager.GetLogger(typeof(SqlQueuesTransport));
        private TimeoutAction timeout;
        private ISqlQueue queue;

        public SqlQueuesTransport(Uri endpoint,
            IEndpointRouter endpointRouter,
            IMessageSerializer messageSerializer,
            int threadCount,
            string connectionString,
            int numberOfRetries,
            IMessageBuilder<MessagePayload> messageBuilder)
        {
            this.endpoint = endpoint;
            this.numberOfRetries = numberOfRetries;
            this.messageBuilder = messageBuilder;
            this.endpointRouter = endpointRouter;
            this.messageSerializer = messageSerializer;
            this.threadCount = threadCount;
            this.connectionString = connectionString;

            queueName = endpoint.GetQueueName();

            threads = new Thread[threadCount];

            // This has to be the first subscriber to the transport events
            // in order to successfuly handle the errors semantics
            new ErrorAction(numberOfRetries).Init(this);
            messageBuilder.Initialize(Endpoint);
        }

        public void Dispose()
        {
            shouldContinue = false;
            logger.DebugFormat("Stopping transport for {0}", endpoint);

            if (timeout != null)
                timeout.Dispose();
            DisposeQueueManager();

            if (!haveStarted)
                return;

            foreach (var thread in threads)
            {
                thread.Join();
            }
        }

        private void DisposeQueueManager()
        {
            if (_sqlQueueManager != null)
            {
                const int retries = 5;
                int tries = 0;
                bool disposeRudely = false;
                while (true)
                {
                    try
                    {
                        _sqlQueueManager.Dispose();
                        break;
                    }
                    catch (Exception)
                    {
                        tries += 1;
                        if (tries > retries)
                        {
                            disposeRudely = true;
                            break;
                        }
                    }
                }
                if (disposeRudely)
                    _sqlQueueManager.DisposeRudely();
            }
        }

        [CLSCompliant(false)]
        public IQueue Queue
        {
            get { return queue; }
        }

        public void Start()
        {
            if (haveStarted)
                return;

            shouldContinue = true;

            _sqlQueueManager = new SqlQueueManager(endpoint, connectionString);
            _sqlQueueManager.CreateQueues(queueName);

            queue = _sqlQueueManager.GetQueue(queueName);

            timeout = new TimeoutAction(queue);
            logger.DebugFormat("Starting {0} threads to handle messages on {1}, number of retries: {2}",
                threadCount, endpoint, numberOfRetries);
            for (var i = 0; i < threadCount; i++)
            {
                threads[i] = new Thread(ReceiveMessage)
                {
                    Name = "Rhino Service Bus Worker Thread #" + i,
                    IsBackground = true
                };
                threads[i].Start(i);
            }
            haveStarted = true;
            var started = Started;
            if (started != null)
                started();
        }

        private void ReceiveMessage(object context)
        {
            int sleepTime = 1;
            int iteration = 0;
            while (shouldContinue)
            {
                Thread.Sleep(sleepTime);
                iteration++;
                try
                {
                    using (var tx = _sqlQueueManager.BeginTransaction())
                    {
                        if (!_sqlQueueManager.Peek(queueName))
                        {
                            sleepTime = Math.Min(sleepTime * iteration * 80,5000);
                            continue;
                        }
                        sleepTime = 1;
                        iteration = 1;
                        tx.Transaction.Commit();
                    }
                }
                catch (TimeoutException)
                {
                    logger.DebugFormat("Could not find a message on {0} during the timeout period",
                                       endpoint);
                    continue;
                }
                catch (ObjectDisposedException)
                {
                    logger.DebugFormat("Shutting down the transport for {0} thread {1}", endpoint, context);
                    return;
                }
                catch (SqlException e)
                {
                    logger.Error(
                        "An error occured while recieving a message, shutting down message processing thread", e);
                    return;
                }

                if (shouldContinue == false)
                    return;

                Message message;
                try
                {
                    using (var tx = _sqlQueueManager.BeginTransaction())
                    {
                        message = _sqlQueueManager.Receive(queueName, TimeSpan.FromSeconds(10));
                        tx.Transaction.Commit();
                    }
                }
                catch (TimeoutException)
                {
                    logger.DebugFormat("Could not find a message on {0} during the timeout period",
                                       endpoint);
                    continue;
                }
                catch (Exception e)
                {
                    logger.Error(
                        "An error occured while recieving a message, shutting down message processing thread",
                        e);
                    return;
                }

                if (message.ProcessedCount > numberOfRetries)
                {
                    Queue.MoveTo(SubQueue.Errors.ToString(), message);
                    Queue.EnqueueDirectlyTo(SubQueue.Errors.ToString(), new MessagePayload
                    {
                        Data = null,
                        Headers = new NameValueCollection
                        {
                            {"correlation-id", message.Id.ToString()},
                            {"retries", message.ProcessedCount.ToString(CultureInfo.InvariantCulture)}
                        }
                    });
                    return;
                }

                var messageWithTimer = new MessageWithTimer {Message = message};
                var messageProcessingTimer = new Timer(extendMessageLeaaseIfMessageStillInProgress, messageWithTimer,
                                                       TimeSpan.FromSeconds(40), TimeSpan.FromMilliseconds(-1));
                messageWithTimer.Timer = messageProcessingTimer;
                
                try
                {
                    var msgType = (MessageType) Enum.Parse(typeof (MessageType), message.Headers["type"]);
                    logger.DebugFormat("Starting to handle message {0} of type {1} on {2}",
                                       message.Id,
                                       msgType,
                                       endpoint);
                    switch (msgType)
                    {
                        case MessageType.AdministrativeMessageMarker:
                            ProcessMessage(message,
                                           AdministrativeMessageArrived,
                                           AdministrativeMessageProcessingCompleted,
                                           null,
                                           null);
                            break;
                        case MessageType.ShutDownMessageMarker:
                            //ignoring this one
                            using(var tx = _sqlQueueManager.BeginTransaction())
                            {
                                _sqlQueueManager.MarkMessageAsReady(message);
                                tx.Transaction.Commit();
                            }
                            break;
                        case MessageType.TimeoutMessageMarker:
                            var timeToSend = XmlConvert.ToDateTime(message.Headers["time-to-send"],
                                                                   XmlDateTimeSerializationMode.Utc);
                            if (timeToSend > DateTime.Now)
                            {
                                timeout.Register(message);
                                using (var tx = queue.BeginTransaction())
                                {
                                    queue.MoveTo(SubQueue.Timeout.ToString(), message);
                                    tx.Transaction.Commit();
                                }
                            }
                            else
                            {
                                ProcessMessage(message,
                                               MessageArrived,
                                               MessageProcessingCompleted,
                                               BeforeMessageTransactionCommit,
                                               BeforeMessageTransactionRollback);
                            }
                            break;
                        default:
                            ProcessMessage(message,
                                           MessageArrived,
                                           MessageProcessingCompleted,
                                           BeforeMessageTransactionCommit,
                                           BeforeMessageTransactionRollback);
                            break;
                    }
                }
                catch (Exception exception)
                {
                    logger.Debug("Could not process message", exception);
                }
                message.FinishedProcessing = true;
            }
        }

        private void extendMessageLeaaseIfMessageStillInProgress(object state)
        {
            var message = state as MessageWithTimer;
            if (message == null) return;

            if (message.Message.FinishedProcessing)
            {
                message.Timer.Dispose();
                message.Timer = null;
                return;
            }

            using (var tx = _sqlQueueManager.BeginTransaction())
            {
                _sqlQueueManager.ExtendMessageLease(message.Message);
                message.Timer.Change(TimeSpan.FromMinutes(9.5), TimeSpan.FromMilliseconds(-1));
                tx.Transaction.Commit();
            }
        }

        private void ProcessMessage(
            Message message,
            Func<CurrentMessageInformation, bool> messageRecieved,
            Action<CurrentMessageInformation, Exception> messageCompleted,
            Action<CurrentMessageInformation> beforeTransactionCommit,
            Action<CurrentMessageInformation> beforeTransactionRollback)
        {
            Exception ex = null;
            try
            {
                //deserialization errors do not count for module events
                object[] messages = DeserializeMessages(message);
                try
                {
                    var messageId = new Guid(message.Headers["id"]);
                    var source = new Uri(message.Headers["source"]);
                    foreach (var msg in messages)
                    {
                        currentMessageInformation = new SqlQueueCurrentMessageInformation
                        {
                            AllMessages = messages,
                            Message = msg,
                            Destination = endpoint,
                            MessageId = messageId,
                            Source = source,
                            TransportMessageId = message.Id.ToString(),
                            Queue = queue,
                            TransportMessage = message
                        };

                        if (TransportUtil.ProcessSingleMessage(currentMessageInformation, messageRecieved) == false)
                            Discard(currentMessageInformation.Message);
                    }
                }
                catch (Exception e)
                {
                    ex = e;
                    logger.Error("Failed to process message", e);
                }
            }
            catch (Exception e)
            {
                ex = e;
                logger.Error("Failed to deserialize message", e);
            }
            finally
            {
                var messageHandlingCompletion = new SqlMessageHandlingCompletion(_sqlQueueManager, null, ex, messageCompleted, beforeTransactionCommit, beforeTransactionRollback, logger,
                                                                              MessageProcessingFailure, currentMessageInformation);
                messageHandlingCompletion.HandleMessageCompletion();
                currentMessageInformation = null;
            }
        }

        private void Discard(object message)
        {
            logger.DebugFormat("Discarding message {0} ({1}) because there are no consumers for it.",
                message, currentMessageInformation.TransportMessageId);
            Send(new Endpoint { Uri = endpoint.AddSubQueue(SubQueue.Discarded) }, new[] { message });
        }

        private object[] DeserializeMessages(Message message)
        {
            try
            {
                return messageSerializer.Deserialize(new MemoryStream(message.Data));
            }
            catch (Exception e)
            {
                try
                {
                    logger.Error("Error when serializing message", e);
                    var serializationError = MessageSerializationException;
                    if (serializationError != null)
                    {
                        currentMessageInformation = new SqlQueueCurrentMessageInformation
                        {
                            Message = message,
                            Source = new Uri(message.Headers["source"]),
                            MessageId = new Guid(message.Headers["id"]),
                            TransportMessageId = message.Id.ToString(),
                            TransportMessage = message,
                            Queue = queue,
                        };
                        serializationError(currentMessageInformation, e);
                    }
                }
                catch (Exception moduleEx)
                {
                    logger.Error("Error when notifying about serialization exception", moduleEx);
                }
                throw;
            }
        }

        public Endpoint Endpoint
        {
            get { return endpointRouter.GetRoutedEndpoint(endpoint); }
        }

        public int ThreadCount
        {
            get { return threadCount; }
        }

        public CurrentMessageInformation CurrentMessageInformation
        {
            get { return currentMessageInformation; }
        }

        public void Send(Endpoint destination, object[] msgs)
        {
            SendInternal(msgs, destination, nv => { });
        }

        private void SendInternal(object[] msgs, Endpoint destination, Action<NameValueCollection> customizeHeaders)
        {
            var messageId = Guid.NewGuid();
            var payload = messageBuilder.BuildFromMessageBatch(msgs);
            logger.DebugFormat("Sending a message with id '{0}' to '{1}'", messageId, destination.Uri);
            customizeHeaders(payload.Headers);

            _sqlQueueManager.Send(destination.Uri, payload);

            var copy = MessageSent;
            if (copy == null)
                return;

            copy(new SqlQueueCurrentMessageInformation
            {
                AllMessages = msgs,
                Source = Endpoint.Uri,
                Destination = destination.Uri,
                MessageId = messageId,
            });
        }

        public void Send(Endpoint endpoint, DateTime processAgainAt, object[] msgs)
        {
            SendInternal(msgs, endpoint,
                nv =>
                {
                    nv["time-to-send"] = processAgainAt.ToString("yyyy-MM-ddTHH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    nv["type"] = MessageType.TimeoutMessageMarker.ToString();
                });
        }

        public void Reply(params object[] messages)
        {
            Send(new Endpoint { Uri = currentMessageInformation.Source }, messages);
        }

        public event Action<CurrentMessageInformation> MessageSent;
        public event Func<CurrentMessageInformation, bool> AdministrativeMessageArrived;
        public event Func<CurrentMessageInformation, bool> MessageArrived;
        public event Action<CurrentMessageInformation, Exception> MessageSerializationException;
        public event Action<CurrentMessageInformation, Exception> MessageProcessingFailure;
        public event Action<CurrentMessageInformation, Exception> MessageProcessingCompleted;
        public event Action<CurrentMessageInformation> BeforeMessageTransactionRollback;
        public event Action<CurrentMessageInformation> BeforeMessageTransactionCommit;
        public event Action<CurrentMessageInformation, Exception> AdministrativeMessageProcessingCompleted;
        public event Action Started;
    }

    public class MessageWithTimer
    {
        public Timer Timer { get; set; }
        public Message Message { get; set; }
    }
}
