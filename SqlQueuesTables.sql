USE [Demoreg_TeleoptiAnalytics]
GO

/****** Object:  Table [Queue].[Messages]    Script Date: 06/01/2012 16:44:40 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [Queue].[Messages](
	[MessageId] [int] IDENTITY(1,1) NOT NULL,
	[QueueId] [int] NOT NULL,
	[CreatedAt] [datetime] NOT NULL,
	[ProcessingUntil] [datetime] NOT NULL,
	[ExpiresAt] [datetime] NULL,
	[Processed] [bit] NOT NULL,
	[Headers] [nvarchar](2000) NULL,
	[Payload] [varbinary](max) NULL,
 CONSTRAINT [PK_Messages] PRIMARY KEY CLUSTERED 
(
	[MessageId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

USE [Demoreg_TeleoptiAnalytics]
GO

/****** Object:  Table [Queue].[Queues]    Script Date: 06/01/2012 16:44:40 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [Queue].[Queues](
	[QueueName] [nvarchar](50) NOT NULL,
	[QueueId] [int] IDENTITY(1,1) NOT NULL,
	[ParentQueueId] [int] NULL,
	[Endpoint] [nvarchar](250) NOT NULL,
 CONSTRAINT [PK_Queues_1] PRIMARY KEY CLUSTERED 
(
	[QueueId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

USE [Demoreg_TeleoptiAnalytics]
GO

/****** Object:  Table [Queue].[SubscriptionStorage]    Script Date: 06/01/2012 16:44:40 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [Queue].[SubscriptionStorage](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Key] [nvarchar](250) NOT NULL,
	[Value] [varbinary](max) NOT NULL,
 CONSTRAINT [PK_SubscriptionStorage1] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

ALTER TABLE [Queue].[Messages] ADD  CONSTRAINT [DF_Messages_CreatedAt]  DEFAULT (getdate()) FOR [CreatedAt]
GO

ALTER TABLE [Queue].[Messages] ADD  CONSTRAINT [DF_Messages_ProcessingUntil]  DEFAULT (getdate()) FOR [ProcessingUntil]
GO

ALTER TABLE [Queue].[Messages] ADD  CONSTRAINT [DF_Messages_Processed]  DEFAULT ((0)) FOR [Processed]
GO

ALTER TABLE [Queue].[Queues] ADD  CONSTRAINT [DF_Queues_Endpoint]  DEFAULT ('') FOR [Endpoint]
GO

