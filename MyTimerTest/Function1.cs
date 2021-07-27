using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace MyTimerTest
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static async Task Run([TimerTrigger("0 */2 * * * *")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"Reggie Timer trigger function executed at: {DateTime.Now}");
            //log.LogInformation($"Reggie Config: {Environment.GetEnvironmentVariable("Database:DataSource")}");
            //log.LogInformation($"Reggie test: {Environment.GetEnvironmentVariable("version")}");

            ServiceBusClient client = new ServiceBusClient(Environment.GetEnvironmentVariable("SBConnectString"));
            ServiceBusSender sender = client.CreateSender(Environment.GetEnvironmentVariable("QName"));

            ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();

            builder.DataSource = Environment.GetEnvironmentVariable("DataSource");
            builder.UserID = Environment.GetEnvironmentVariable("UserID");
            builder.Password = Environment.GetEnvironmentVariable("Password");
            builder.InitialCatalog = Environment.GetEnvironmentVariable("SchedulerDB");

            log.LogInformation($"DB configuration source:{builder.DataSource} userId:{builder.UserID} pwd:{builder.Password} db:{builder.InitialCatalog}");

            SqlConnection sqlConnection = new SqlConnection(builder.ConnectionString);
            sqlConnection.Open();

            string sql = @"select Id, BotId, UserId, userName, ChannleId, Locale, ServiceUrl from Conversations " +
                "where ChannleId = 'msteams'";

            using (SqlCommand command = new SqlCommand(sql, sqlConnection))
            {
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var id = reader.GetString(0);
                        var botId = reader.GetString(1);
                        var userId = reader.GetString(2);
                        var userName = reader.GetString(3);
                        var channelId = reader.GetString(4);
                        var locale = reader.GetString(5);
                        var serviceUrl = reader.GetString(6);

                        log.LogInformation($"DB record id:{id} botId:{botId} userId:{userId} userName:{userName} channelId:{channelId} local:{locale} serviceUrl:{serviceUrl}");
                        string record = $"{id}|{botId}|{userId}|{userName}|{channelId}|{locale}|{serviceUrl}";

                        if(!messageBatch.TryAddMessage(new ServiceBusMessage(record)))
                        {
                            throw new Exception("To large for a message");
                        }
                    }
                }
            }

            try
            {
                //await sender.SendMessagesAsync(messageBatch);
                log.LogInformation("Send a new message");
            }
            finally
            {
                await sender.DisposeAsync();
                await client.DisposeAsync();
            }
        }
    }
}
