using System;
using System.Configuration;
using Rhino.ServiceBus.Impl;

namespace Rhino.ServiceBus.Config
{
    public class SqlQueuesConfigurationAware : IBusConfigurationAware
    {
        public void Configure(AbstractRhinoServiceBusConfiguration configuration, IBusContainerBuilder builder)
        {
            var busConfig = configuration as RhinoServiceBusConfiguration;
            if (busConfig == null)
                return;

            if (configuration.Endpoint.Scheme.Equals("sql.queues", StringComparison.InvariantCultureIgnoreCase) ==
                false)
                return;

            var busConfigSection = configuration.ConfigurationSection.Bus;

            if (string.IsNullOrEmpty(busConfigSection.Name))
                throw new ConfigurationErrorsException(
                    "Could not find attribute 'name' in node 'bus' in configuration");

            if (string.IsNullOrEmpty(busConfigSection.ConnectionString))
                throw new ConfigurationErrorsException(
                    "Could not find attribute 'connectionString' in node 'bus' in configuration");

            builder.RegisterSqlQueuesTransport();
        }
    }
}