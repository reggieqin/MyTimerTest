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
        public static async Task Run([TimerTrigger("0 */30 * * * *")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"Reggie Timer trigger function executed at: {DateTime.Now}");

            DataTable userData = await getUserDataFromDB(log);

            await SendUserDataToMQ(userData, log);
        }

        public static async Task<DataTable> getUserDataFromDB(ILogger log)
        {
            SecretClient secretClient = new SecretClient(new Uri(Environment.GetEnvironmentVariable("KeyVaultEndpoint")), new DefaultAzureCredential());
            KeyVaultSecret userId = await secretClient.GetSecretAsync($"SQL-userId");
            KeyVaultSecret password = await secretClient.GetSecretAsync($"SQL-password");

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = Environment.GetEnvironmentVariable("DataSource");
            builder.UserID = userId.Value;
            builder.Password = password.Value;
            builder.InitialCatalog = Environment.GetEnvironmentVariable("SchedulerDB");

            SqlConnection sqlConnection = new SqlConnection(builder.ConnectionString);
            sqlConnection.Open();

            DataTable dataTable = new DataTable();

            string userDataSql = $"EXEC GetUserData";

            log.LogInformation($"Start executing sql {userDataSql}");

            using (SqlCommand command = new SqlCommand(userDataSql, sqlConnection))
            {
                using (SqlDataAdapter da = new SqlDataAdapter(command))
                {
                    try
                    {
                        da.Fill(dataTable);
                        log.LogInformation("Retrieving data from SQL successfully");
                    }
                    catch(Exception e)
                    {
                        log.LogError($"Retrieving data from SQL failed due to {e.Message}");
                    }
                    finally
                    {
                        da.Dispose();
                    }
                }
            }

            sqlConnection.Close();

            return dataTable;
        }

        public static async Task SendUserDataToMQ(DataTable userData, ILogger log)
        {
            List<ServiceBusMessage> serviceBusMessages = new List<ServiceBusMessage>();
            foreach (DataRow row in userData.Rows)
            {
                string id = row[userData.Columns[0]] as string;
                string appId = row[userData.Columns[1]] as string;

                var serviceBugMessage = new ServiceBusMessage(Encoding.UTF8.GetBytes("meetingbrief"));
                serviceBugMessage.ApplicationProperties.Add("id", id);
                serviceBugMessage.ApplicationProperties.Add("appId", appId);
                serviceBugMessage.TimeToLive = new TimeSpan(2, 0, 0, 0);

                serviceBusMessages.Add(serviceBugMessage);

                log.LogInformation($"Adding message {id} {appId}");
            }

            if (serviceBusMessages.Count > 0)
            {
                var _client = new ServiceBusClient(Environment.GetEnvironmentVariable("SBConnectString"));
                var _clientSender = _client.CreateSender(Environment.GetEnvironmentVariable("TopicName"));

                await _clientSender.SendMessagesAsync(serviceBusMessages).ConfigureAwait(false);

                log.LogInformation($"Total {serviceBusMessages.Count} messages sent");
            }
            else
            {
                log.LogInformation("No message to send");
            }
        }
    }
}
