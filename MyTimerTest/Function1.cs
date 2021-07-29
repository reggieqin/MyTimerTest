using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using System.Linq;
using Microsoft.Azure.ServiceBus;
using System.Text;
using System.Collections.Generic;
using Azure.Security.KeyVault.Secrets;
using Azure.Identity;


namespace MyTimerTest
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static async Task Run([TimerTrigger("0 */2 * * * *")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"Reggie Timer trigger function executed at: {DateTime.Now}");
            //log.LogInformation($"Reggie Config: {Environment.GetEnvironmentVariable("Database:DataSource")}");
            //log.LogInformation($"Reggie test: {Environment.GetEnvironmentVariable("version")}");

            //ServiceBusClient client = new ServiceBusClient(Environment.GetEnvironmentVariable("SBConnectString"));
            //ServiceBusSender sender = client.CreateSender(Environment.GetEnvironmentVariable("QName"));
            //ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

            SecretClient secretClient = new SecretClient(new Uri(Environment.GetEnvironmentVariable("KeyVaultEndpoint")), new DefaultAzureCredential());
            KeyVaultSecret userId = await secretClient.GetSecretAsync($"SQL-userId");
            KeyVaultSecret password = await secretClient.GetSecretAsync($"SQL-password");

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = Environment.GetEnvironmentVariable("DataSource");
            builder.UserID = userId.Value;
            builder.Password = password.Value;
            builder.InitialCatalog = Environment.GetEnvironmentVariable("SchedulerDB");

            log.LogInformation($"DB configuration source:{builder.DataSource} userId:{builder.UserID} pwd:{builder.Password} db:{builder.InitialCatalog}");

            SqlConnection sqlConnection = new SqlConnection(builder.ConnectionString);
            sqlConnection.Open();

            // Get unique appId
            List<string> appList = new List<string>();
            string uniqueAppidSql = @"select distinct AppId from userinfo";
            using (SqlCommand command = new SqlCommand(uniqueAppidSql, sqlConnection))
            {
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var appId = reader.GetString(0);

                        log.LogInformation($"Getting unique appId {appId} start creating...");

                        var managementClient = new ManagementClient(Environment.GetEnvironmentVariable("SBConnectString"));

                        var allQueues = await managementClient.GetQueuesAsync();

                        var foundQueue = allQueues.Where(q => q.Path == appId.ToLower()).SingleOrDefault();

                        if (foundQueue == null)
                        {
                            await managementClient.CreateQueueAsync(appId);//add queue desciption properties

                            log.LogInformation($"Queue {appId} is created");
                        }
                        else
                        {
                            log.LogInformation($"Skip creating queue {appId} which already exists");
                        }

                        appList.Add(appId);
                    }
                }
            }

            foreach(string appId in appList)
            {
                var queueClient = new QueueClient(Environment.GetEnvironmentVariable("SBConnectString"), appId);

                log.LogInformation($"Start sending message to queue {appId}");

                string sql = @"select Id, TimeOffset, WorkHour from UserInfo where appId = '" + appId + "'";

                using (SqlCommand command = new SqlCommand(sql, sqlConnection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var id = reader.GetString(0);
                            var timeOffset = reader.GetInt32(1);
                            var workHour = reader.GetInt32(2);

                            log.LogInformation($"DB record id:{id} timeOffset:{timeOffset} workHour:{workHour}");
                            string record = $"{id}";

                            var message = new Message(Encoding.UTF8.GetBytes(record));
                            await queueClient.SendAsync(message);

                            log.LogInformation($"New message sent {record}");
                        }
                    }
                }

                await queueClient.CloseAsync();
            }
        }
    }
}
