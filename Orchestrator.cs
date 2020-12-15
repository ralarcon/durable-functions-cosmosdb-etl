using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

namespace Ragc.Etl
{
    public static class Orchestrator
    {
        private const string ExternalEndpointUrl = "https://testpyfunc-weu.azurewebsites.net/api/list";
        private const string UrlParams = "?code=TNaBHahpBoa4scaJbRcx3YHk/7NVLu6gEYaUuZhViJeQ6zN9MN0tKg==";

        private static CosmosClient _cosmosClient = new CosmosClient(Environment.GetEnvironmentVariable("CosmosDbConnStr"));
        private static Database _db = _cosmosClient.GetDatabase("documents");

        /// Orchestration Entry Point (done with an scheduled function)
        /// Considerations: We can have the orchestration being executed every, let say 5 mins. 
        //  Improvements: every execution verify if it is needed to run (check the last excecution and verify if it was succeded, etc...)
        [FunctionName("Orchestration_Run")]
        public static async Task RunExtraction(
            [TimerTrigger("0 */5 * * * *")] TimerInfo myTimer, ILogger log,
            [DurableClient] IDurableOrchestrationClient starter)
        {
            if (myTimer.IsPastDue)
            {
                log.LogInformation("Timer is running late!");
            }
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("Orchestrator", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
        }

        [FunctionName("Orchestrator")]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            try
            {
                //TODO: Move locks to use entity function?

                if (await context.CallActivityAsync<bool>("GetOrchestrationLease", null))
                {
                    // Retrieve Data
                    var items = await context.CallActivityAsync<IEnumerable<SampleItem>>("ExtractDocuments", null);

                    // SaveItems
                    await context.CallActivityAsync<string>("SaveDocuments", items);

                    //Release Lock
                    await context.CallActivityAsync<string>("ReleaseOrchestrationLease", true);
                }
                else{
                    log.LogInformation("Orchestration lease already in place. Skipping execution.");
                }

            }
            catch (Exception ex)
            {
                //Exception handling & compensation if needed
                log.LogError("Orchestration execution failed: " + ex.Message);
                await context.CallActivityAsync<string>("ReleaseOrchestrationLease", false);
                throw;
            }
        }
        [FunctionName("GetOrchestrationLease")]
        public static async Task<bool> GetOrchestrationLeaseAsync([ActivityTrigger] string name, ILogger log)
        {
            Container container = await _db.CreateContainerIfNotExistsAsync("orchestrationLease", "/id");

            OrchestrationLease olease = null;

            try
            {
                olease = await container.ReadItemAsync<OrchestrationLease>("OrchestrationLeaseId", new PartitionKey("OrchestrationLeaseId"));
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                log.LogInformation("OrchestrationLease does not exists. The first orchestration lease item will be created.");
                olease = new OrchestrationLease();
                olease.Id = "OrchestrationLeaseId";
            }

            ItemRequestOptions options = null;
            options = new ItemRequestOptions();
            options.IfMatchEtag = olease.ETag;

            if (!olease.Locked)
            {
                olease.Locked = true;
                olease.PreferredLocations = Environment.GetEnvironmentVariable("PreferredLocations");
                olease.StartTime = DateTime.Now;

                try
                {
                    await container.UpsertItemAsync<OrchestrationLease>(olease, new PartitionKey("OrchestrationLeaseId"), options);
                    return true;
                }
                catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.PreconditionFailed)
                {
                    log.LogInformation($"Unable to set the lock (other process have updated the lease). This execution will be skipped. ETag precondition failed: {ex.ToString()}");
                    return false;
                }
                catch(Exception ex){
                    log.LogError("Error adquiring the orchestration lock:" + ex.Message);
                    throw;
                }
            }
            else
            {
                log.LogInformation($"Lock alrady adquired by other process. This execution will skip.");
                return false;
            }
        }

        [FunctionName("ReleaseOrchestrationLease")]
        public static async Task ReleaseOrchestrationLeaseAsync([ActivityTrigger] bool succeded, ILogger log)
        {
            Container container = await _db.CreateContainerIfNotExistsAsync("orchestrationLease", "/id");
            OrchestrationLease olease = await container.ReadItemAsync<OrchestrationLease>("OrchestrationLeaseId", new PartitionKey("OrchestrationLeaseId"));

            ItemRequestOptions options = null;

            options = new ItemRequestOptions();
            options.IfMatchEtag = olease.ETag;

            if (olease.Locked)
            {
                olease.Locked = false;
                olease.EndTime = DateTime.Now;
                olease.Succeeded = succeded;   
                try
                {
                    await container.UpsertItemAsync<OrchestrationLease>(olease, new PartitionKey("OrchestrationLeaseId"), options);
                }
                catch (Exception ex)
                {
                    log.LogError($"Error releasing the lock: {ex.Message}");
                    throw;
                }
            }
            else{
                log.LogWarning("Lock is not adquired. Not releasing.");
            }
        }


        [FunctionName("ExtractDocuments")]
        public static async Task<IEnumerable<SampleItem>> ExtractAsync([ActivityTrigger] string name, ILogger log)
        {
            try
            {
                //Call The HTTP EndPoint to retrieve JSon data
                HttpClient client = new HttpClient();
                client.BaseAddress = new Uri(ExternalEndpointUrl);
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                HttpResponseMessage response = await client.GetAsync(UrlParams);
                if (response.IsSuccessStatusCode)
                {
                    var dataItems = await response.Content.ReadAsAsync<IEnumerable<SampleItem>>();

                    return dataItems;
                }
                else
                {
                    log.LogError($"Unable to retrieve data from { ExternalEndpointUrl }. Status Code: {response.StatusCode}.");
                    return null;
                }
            }
            catch (Exception ex)
            {
                log.LogError($"Exception in ExtractAndStore: {ex.Message}");
                return null;
            }
        }

        [FunctionName("SaveDocuments")]
        public static async Task SaveDocumentsAsync([ActivityTrigger] IEnumerable<SampleItem> itemsIn,
            [CosmosDB(databaseName: "documents", collectionName: "extracted", ConnectionStringSetting = "CosmosDbConnStr", CreateIfNotExists = true,
            PreferredLocations="%PreferredLocations%", UseMultipleWriteLocations=true )]IAsyncCollector<SampleItem> itemsOut,
            ILogger log)
        {
            log.LogInformation($"Saving items...");
            int itemsCount = 0;
            foreach (SampleItem item in itemsIn)
            {
                log.LogInformation($"Document Name={item.Name}");
                item.Id = Guid.NewGuid();
                await itemsOut.AddAsync(item);
                itemsCount++;
            }
            log.LogInformation($"Saved {itemsCount} items");
        }

        [FunctionName("TransformDocument")]
        public static async Task TransformDocumentAsync(
            [CosmosDBTrigger(databaseName: "documents", collectionName: "extracted", ConnectionStringSetting = "CosmosDbConnStr",
            LeaseCollectionName = "transformLease",
            CreateLeaseCollectionIfNotExists=true,
            PreferredLocations="%PreferredLocations%", UseMultipleWriteLocations=true )]IReadOnlyList<Microsoft.Azure.Documents.Document> documents,
            [CosmosDB(databaseName: "documents", collectionName: "transformed", ConnectionStringSetting = "CosmosDbConnStr", CreateIfNotExists = true,
            PreferredLocations="%PreferredLocations%", UseMultipleWriteLocations=true )]IAsyncCollector<SampleItem> itemsOut,
            ILogger log)
        {
            log.LogInformation($"Transforming documents...");
            int itemsCount = 0;
            Guid batchGuid = Guid.NewGuid();
            foreach (var document in documents)
            {
                itemsCount++;

                SampleItem item = JsonConvert.DeserializeObject<SampleItem>(document.ToString());

                log.LogInformation($"Transforming item {item.Id}. Setting ");
                item.UpdateLocation = Environment.GetEnvironmentVariable("PreferredLocations");
                item.TransformData = $"Data added from transform function [{itemsCount}]";
                item.TransformBatch = batchGuid;
                await itemsOut.AddAsync(item);
            }
            log.LogInformation($"Transformed {itemsCount} items");
        }
    }
}