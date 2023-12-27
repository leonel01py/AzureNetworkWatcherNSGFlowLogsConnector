using Azure.Data.Tables;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading.Tasks;

namespace Cortex {

    class Program {
        static async Task Main(string[] args) {
            Console.WriteLine($"Cortex Worker (PID: { Environment.ProcessId }) initialized.");

            var host = new HostBuilder()
                .ConfigureFunctionsWorkerDefaults()
                .ConfigureServices(services =>
                {
                    services.AddApplicationInsightsTelemetryWorkerService();
                    services.ConfigureFunctionsApplicationInsights();
                })
                .ConfigureAppConfiguration((config) => {
                    var connectionString = Util.GetEnvironmentVariable("AzureWebJobsStorage");
                    var serviceClient = new TableServiceClient(connectionString);
                    var tableClient = serviceClient.GetTableClient("checkpoints");

                    // Create the table if it doesn't exist.
                    var table = tableClient.CreateIfNotExists();
                })
                .Build();


            await host.RunAsync();
        }

    }
}


