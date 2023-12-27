using Azure.Data.Tables;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Cortex;

Console.WriteLine($"Cortex Worker (PID: { Environment.ProcessId }) initialized.");

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(services =>
    {
        services.AddApplicationInsightsTelemetryWorkerService();
        services.ConfigureFunctionsApplicationInsights();
    })
    .ConfigureAppConfiguration(async (config) => {
        var connectionString = Util.GetEnvironmentVariable("AzureWebJobsStorage");
        var serviceClient = new TableServiceClient(connectionString);
        var tableClient = serviceClient.GetTableClient("checkpoints");

        // Create the table if it doesn't exist.
        var table = tableClient.CreateIfNotExists();
    })
    .Build();

host.Run();
