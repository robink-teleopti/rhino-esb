using System;
using Rhino.ServiceBus.Transport;
using System.Linq;

namespace Rhino.ServiceBus.Util
{
    public static class UriExtensions
    {
        public static Uri AddSubQueue(this Uri self, SubQueue subQueue)
        {
            Uri subQueueUrl;
            if (self.AbsolutePath.EndsWith("/"))
                subQueueUrl = new Uri(self + subQueue.ToString());
            else
                subQueueUrl = new Uri(self + "/" + subQueue);
            return subQueueUrl;
        }

        public static string GetQueueName(this Uri self)
        {
            return self.AbsolutePath.Substring(1).Split('/').First();
        }
    }
}