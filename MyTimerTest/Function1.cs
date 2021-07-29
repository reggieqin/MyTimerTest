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
        public static async Task Run([TimerTrigger("* 0 * * * *")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"Reggie Timer trigger function executed at: {DateTime.Now}");

            SecretClient secretClient = new SecretClient(new Uri(Environment.GetEnvironmentVariable("KeyVaultEndpoint")), new DefaultAzureCredential());
            KeyVaultSecret userId = await secretClient.GetSecretAsync($"SQL-userId");
            KeyVaultSecret password = await secretClient.GetSecretAsync($"SQL-password");

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = Environment.GetEnvironmentVariable("DataSource");
            builder.UserID = userId.Value;
            builder.Password = password.Value;
            builder.InitialCatalog = Environment.GetEnvironmentVariable("SchedulerDB");

            log.LogInformation($"DB configuration source:{builder.DataSource} userId:{builder.UserID} pwd:{builder.Password} db:{builder.InitialCatalog}");

            DateTime currentUTCDatetime = DateTime.Now.ToUniversalTime();
            string currentUTCTime = currentUTCDatetime.ToString("HH:00:00");

            SqlConnection sqlConnection = new SqlConnection(builder.ConnectionString);
            sqlConnection.Open();

            // Get unique appId
            IDictionary<string, IList<string>> appUserDataDict = new Dictionary<string, IList<string>>();
            string userDataSql = $"select Id, AppId, LastSentTime from userinfo where WorkHour = '{currentUTCTime}'";
            if (Environment.GetEnvironmentVariable("testonly") == "yes")
            {
                userDataSql = $"select Id, AppId, LastSentTime from userinfo";
            }

            log.LogInformation($"Query string {userDataSql}");

            using (SqlCommand command = new SqlCommand(userDataSql, sqlConnection))
            {
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var id = reader.GetString(0);
                        var appId = reader.GetString(1);
                        if (!reader.IsDBNull(2))
                        {
                            var lastSentTime = reader.GetDateTime(2);
                            if (currentUTCDatetime.Date == lastSentTime.Date)
                            {
                                log.LogInformation("Do not send duplicate message again in the same day");
                                continue;
                            }
                        }

                        if (!appUserDataDict.ContainsKey(appId))
                        {
                            appUserDataDict[appId] = new List<string>();
                        }

                        appUserDataDict[appId].Add(id);

                        log.LogInformation($"user {id} added in app {appId}");
                    }
                }
            }

            foreach(var app in appUserDataDict)
            {
                log.LogInformation($"Getting unique appId {app.Key} start creating...");

                var managementClient = new ManagementClient(Environment.GetEnvironmentVariable("SBConnectString"));

                var allQueues = await managementClient.GetQueuesAsync();

                var foundQueue = allQueues.Where(q => q.Path == app.Key.ToLower()).SingleOrDefault();

                if (foundQueue == null)
                {
                    await managementClient.CreateQueueAsync(app.Key);//add queue desciption properties

                    log.LogInformation($"Queue {app.Key} is created");
                }
                else
                {
                    log.LogInformation($"Skip creating queue {app.Key} which already exists");
                }

                var queueClient = new QueueClient(Environment.GetEnvironmentVariable("SBConnectString"), app.Key);

                foreach (var userData in app.Value)
                {
                    string record = $"{userData}";

                    var message = new Message(Encoding.UTF8.GetBytes(record));
                    await queueClient.SendAsync(message);

                    log.LogInformation($"New message sent {record}");
                }
            }
        }
    }
}
