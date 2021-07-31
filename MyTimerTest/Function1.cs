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
using System.Data;
using Microsoft.ServiceBus.Messaging;

namespace MyTimerTest
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static async Task Run([TimerTrigger("0 * * * * *")] TimerInfo myTimer,
            [ServiceBus("meetingbrief", Connection = "SBConnectString", 
            EntityType = Microsoft.Azure.WebJobs.ServiceBus.EntityType.Topic)] IAsyncCollector<ServiceBusMessage> output, 
            ILogger log)
        {
            log.LogInformation($"Reggie Timer trigger function executed at: {DateTime.Now}");

            try
            {
                var message = new ServiceBusMessage(Encoding.UTF8.GetBytes("test"));
                message.ApplicationProperties.Add("id", "123");
                message.ApplicationProperties.Add("appId", "1234");
                message.ApplicationProperties.Add("goal", 10);
                message.CorrelationId = "1234";
                message.SessionId = "1234";
                message.Subject = "1234";
                message.TimeToLive = new TimeSpan(2, 0, 0, 0);

                await output.AddAsync(message);

                var message2 = new ServiceBusMessage(Encoding.UTF8.GetBytes("test2"));
                message2.ApplicationProperties.Add("id", "123");
                message2.ApplicationProperties.Add("appId", "5678");
                message2.ApplicationProperties.Add("goal", 5);
                message2.CorrelationId = "5678";
                message2.SessionId = "5678";
                message2.Subject = "5678"; 
                message2.TimeToLive = new TimeSpan(2, 0, 0, 0);

                await output.(message2);
            }
            catch(Exception e)
            {
                log.LogError("Error " + e.Message);
            }


            //SecretClient secretClient = new SecretClient(new Uri(Environment.GetEnvironmentVariable("KeyVaultEndpoint")), new DefaultAzureCredential());
            //KeyVaultSecret userId = await secretClient.GetSecretAsync($"SQL-userId");
            //KeyVaultSecret password = await secretClient.GetSecretAsync($"SQL-password");

            //SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            //builder.DataSource = Environment.GetEnvironmentVariable("DataSource");
            //builder.UserID = userId.Value;
            //builder.Password = password.Value;
            //builder.InitialCatalog = Environment.GetEnvironmentVariable("SchedulerDB");

            //log.LogInformation($"DB configuration source:{builder.DataSource} userId:{builder.UserID} pwd:{builder.Password} db:{builder.InitialCatalog}");

            //DateTime currentUTCDatetime = DateTime.Now.ToUniversalTime();
            //string currentUTCTime = currentUTCDatetime.ToString("HH:00:00");

            //SqlConnection sqlConnection = new SqlConnection(builder.ConnectionString);
            //sqlConnection.Open();

            //// Get unique appId
            //IDictionary<string, IList<string>> appUserDataDict = new Dictionary<string, IList<string>>();

            //DataTable dataTable = new DataTable();

            //string userDataSql = $"EXEC GetUserData";

            //using (SqlCommand command = new SqlCommand(userDataSql, sqlConnection))
            //{
            //    using (SqlDataAdapter da = new SqlDataAdapter(command))
            //    {
            //        try
            //        {
            //            da.Fill(dataTable);

            //            foreach (DataRow row in dataTable.Rows)
            //            {
            //                string appId = row[dataTable.Columns[1]] as string;
            //                if (!appUserDataDict.ContainsKey(appId))
            //                {
            //                    appUserDataDict[appId] = new List<string>();
            //                }
            //                appUserDataDict[appId].Add(row[dataTable.Columns[0]] as string);
            //            }
            //        }
            //        finally
            //        {
            //            da.Dispose();
            //        }

            //    }
            //}

            //sqlConnection.Close();

            //foreach (var app in appUserDataDict)
            //{
            //    log.LogInformation($"Getting unique appId {app.Key} start creating...");

            //    var managementClient = new ManagementClient(Environment.GetEnvironmentVariable("SBConnectString"));

            //    var allQueues = await managementClient.GetQueuesAsync();

            //    var foundQueue = allQueues.Where(q => q.Path == app.Key.ToLower()).SingleOrDefault();

            //    if (foundQueue == null)
            //    {
            //        await managementClient.CreateQueueAsync(app.Key);//add queue desciption properties

            //        log.LogInformation($"Queue {app.Key} is created");
            //    }
            //    else
            //    {
            //        log.LogInformation($"Skip creating queue {app.Key} which already exists");
            //    }

            //    var queueClient = new QueueClient(Environment.GetEnvironmentVariable("SBConnectString"), app.Key);

            //    foreach (var userData in app.Value)
            //    {
            //        string record = $"{userData}";

            //        var message = new Message(Encoding.UTF8.GetBytes(record));
            //        await queueClient.SendAsync(message);

            //        log.LogInformation($"New message sent {record} to {app.Key}");
            //    }
            //}
        }
    }
}
