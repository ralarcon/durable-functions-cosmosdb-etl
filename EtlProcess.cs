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
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using System.Text;

namespace Ragc.Etl
{
    public static class EtlProcess
    {

        private const string TriggerSchedule="0 */2 * * * *"; //each 2 minutes
        private const int OrchestrationLeaseTimeOut = 5; //minutes - we don't expect any execution to last more than 5 minutes (actually it usually takes less than 1 minute -depending on how many docs are processed))

        private static CosmosClient _cosmosClient = new CosmosClient(Environment.GetEnvironmentVariable("CosmosDbConnStr"));
        private static Database _db = _cosmosClient.GetDatabase("documents");

        /// Orchestration Entry Point (done with an scheduled function)
        /// Considerations: We can have the orchestration being executed every, let say 5 mins. 
        //  Improvements: every execution verify if it is needed to run (check the last excecution and verify if it was succeded, etc...)
        [FunctionName("TriggerOrchestration")]
        public static async Task TriggerOrchestration(
            [TimerTrigger(TriggerSchedule)] TimerInfo myTimer, ILogger log,
            [DurableClient] IDurableOrchestrationClient starter)
        {
            if (myTimer.IsPastDue)
            {
                log.LogInformation("Timer is running late!");
            }
            log.LogInformation($"Starting new Orchestrator at {DateTime.Now}");

            string instanceId = await starter.StartNewAsync("Orchestrator", null);

            log.LogInformation($"Orchestration with ID = '{instanceId}'.");
        }

        [FunctionName("Orchestrator")]
        public static async Task ExtractAndSaveDocumentsAysnc(
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
                    await context.CallActivityAsync<string>("ReleaseOrchestrationLease", (true, $"Successful extracted and saved {items.Count()} documents. Documents will be processed automatically by the TransformDocument function."));
                }
                else{
                    log.LogInformation("Orchestration lease already in place. Skipping execution.");
                }

            }
            catch (Exception ex)
            {
                //Exception handling & compensation if needed
                log.LogError("Orchestration execution failed: " + ex.Message);
                await context.CallActivityAsync<string>("ReleaseOrchestrationLease", (false, $"There was errors while executing the orchestration. Exception info: {ex.ToString()}"));
                throw;
            }
        }
        [FunctionName("GetOrchestrationLease")]
        public static async Task<bool> GetOrchestrationLeaseAsync([ActivityTrigger] string name, ILogger log)
        {
            Container container = await _db.CreateContainerIfNotExistsAsync("orchestrationLease", "/id");

            OrchestrationLease olease = await GetLeaseInfoAsync(log, container);

            if (!olease.Locked || olease.Locked && DateTime.Now > olease.LeaseTimeOut)
            {
                return await AdquireLeaseAsync(log, container, olease);
            }
            else
            {
                log.LogInformation($"Lock alrady adquired by other process. This execution will skip.");
                return false;
            }
        }

        [FunctionName("ReleaseOrchestrationLease")]
        public static async Task ReleaseOrchestrationLeaseAsync([ActivityTrigger] (bool succeeded, string additonalInfo) info, 
        [CosmosDB(databaseName: "documents", collectionName: "orchestrationRuns", ConnectionStringSetting = "CosmosDbConnStr", CreateIfNotExists = true, PartitionKey="/id",
            PreferredLocations="%PreferredLocations%", UseMultipleWriteLocations=true )]IAsyncCollector<OrchestrationRun> runs,
            ILogger log)
        {
            Container container = await _db.CreateContainerIfNotExistsAsync("orchestrationLease", "/id");
            OrchestrationLease olease = await container.ReadItemAsync<OrchestrationLease>("OrchestrationLeaseId", new PartitionKey("OrchestrationLeaseId"));

            bool forcedLease = olease.LastLeaseTimedOut;

            //Release the Orchestration Lease
            await ReleaseLeaseAsync(log, container, olease);

            //Update Orchestration Runs
            await runs.AddAsync(new OrchestrationRun()
            {
                Id = Guid.NewGuid(),
                StartTime = olease.StartTime,
                EndTime = olease.EndTime,
                Duration = (olease.EndTime - olease.StartTime).TotalSeconds,
                ForcedLease = forcedLease,
                Succeeded = info.succeeded,
                OrchestrationWorker = olease.Worker,
                AdditionlInfo = info.additonalInfo,
            });
        }

        [FunctionName("ExtractDocuments")]
        public static async Task<IEnumerable<SampleItem>> ExtractAsync([ActivityTrigger] string name, ILogger log)
        {
            try
            {
                //Call The HTTP EndPoint to retrieve JSon data
                HttpClient client = new HttpClient();
                client.BaseAddress = new Uri(Environment.GetEnvironmentVariable("ExternalEndpoint"));
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                HttpResponseMessage response = await client.GetAsync(Environment.GetEnvironmentVariable("ExternalEndpointParams"));
                if (response.IsSuccessStatusCode)
                {
                    var dataItems = await response.Content.ReadAsAsync<IEnumerable<SampleItem>>();

                    return dataItems;
                }
                else
                {
                    log.LogError($"Unable to retrieve data from { Environment.GetEnvironmentVariable("ExternalEndpoint") }. Status Code: {response.StatusCode}.");
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
            [CosmosDB(databaseName: "documents", collectionName: "extracted", ConnectionStringSetting = "CosmosDbConnStr", CreateIfNotExists = true, PartitionKey="/LogicalPartition",
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
            LeaseCollectionName = "transformsLease",
            CreateLeaseCollectionIfNotExists=true,
            MaxItemsPerInvocation=10, //TODO: ADJUST TO SCALING / COST / PERFORMANCE NEEDS --> Tests Required
            PreferredLocations="%PreferredLocations%", UseMultipleWriteLocations=true )]IReadOnlyList<Microsoft.Azure.Documents.Document> documents,
            [CosmosDB(databaseName: "documents", collectionName: "transformed", ConnectionStringSetting = "CosmosDbConnStr", CreateIfNotExists = true, PartitionKey="/id",
            PreferredLocations="%PreferredLocations%", UseMultipleWriteLocations=true )]IAsyncCollector<TransformedItem> itemsOut,
            ILogger log)
        {
            log.LogInformation($"Transforming documents...");
            int itemsCount = 0;
            Guid batchGuid = Guid.NewGuid();
            foreach (var document in documents)
            {
                itemsCount++;

                SampleItem sourceItem = JsonConvert.DeserializeObject<SampleItem>(document.ToString());
                
                log.LogInformation($"Transforming item {sourceItem.Id}");
                TransformedItem targetItem = Transform(itemsCount, batchGuid, sourceItem);

                await Task.Delay(100); //TEST throttle output

                await itemsOut.AddAsync(targetItem);
            }
            log.LogInformation($"Transformed {itemsCount} items.");
        }

        /// Sample function to generate a list of json documents for testing purposes
        [FunctionName("ExternalDocumentList")]
        public static async Task<IActionResult> GenerateSampleDocumentList(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)]
            HttpRequest req, ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            Random rnd = new Random();
            string paramCount = req.Query["count"];
            int itemCount = 0; //default item generation
            int.TryParse(paramCount, out itemCount);
            if(itemCount == 0){
               itemCount = rnd.Next(10, 200);
            }
            
            List<SampleItem> generatedItems = new List<SampleItem>(itemCount);
            for(int i = 1; i<=itemCount; i++){
                SampleItem item = new SampleItem(){
                    Id = Guid.NewGuid(),
                    Date = DateTime.Now.ToString(),
                    Done = "true",
                    Desc = $"Sample item #{i} of {itemCount} generated",
                    Name = $"Item_{DateTime.Now.ToString("yyyyMMhhmm")}_{i}",
                    Pr = $"Additional field info",
                    LogicalPartition = $"Partition_{(char)(65 + i % 3)}"
                };
                generatedItems.Add(item);
            }
            string data = JsonConvert.SerializeObject(generatedItems);
        
            return await Task.FromResult(new JsonResult(generatedItems));
        }

        private static TransformedItem Transform(int itemsCount, Guid batchGuid, SampleItem sourceItem)
        {
            return new TransformedItem()
            {
                Id = sourceItem.Id,
                SourceDate = sourceItem.Date,
                Description = String.IsNullOrWhiteSpace(sourceItem.Desc) ? $"Empty Description in source for item {sourceItem.Id}" : sourceItem.Desc,
                Name = String.IsNullOrWhiteSpace(sourceItem.Name) ? $"Empty Name in source for item {sourceItem.Id}" : sourceItem.Name,
                AdditionalData = $"Data added from transform function [{itemsCount}]",
                Done = sourceItem.Done,
                Pr = sourceItem.Pr,
                TransformBatch = batchGuid,
                UpdateLocation = Environment.GetEnvironmentVariable("PreferredLocations")?.Split(",")[0],
                TransformTimeStamp = DateTime.Now
            };
        }

        private static async Task<OrchestrationLease> GetLeaseInfoAsync(ILogger log, Container container)
        {
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

            return olease;
        }

        private static async Task<bool> AdquireLeaseAsync(ILogger log, Container container, OrchestrationLease olease)
        {
            ItemRequestOptions options = null;
            options = new ItemRequestOptions();
            options.IfMatchEtag = olease.ETag;

            if (olease.Locked && DateTime.Now > olease.LeaseTimeOut)
            {
                //The lease granted has timed out
                olease.LastLeaseTimedOut = true;
                olease.EndTime = DateTime.Now;
                log.LogWarning("Previous lease timed out. Reseting the lease.");
            }

            olease.Locked = true;
            olease.Worker = $"{Environment.GetEnvironmentVariable("PreferredLocations")?.Split(",")[0]}";
            olease.StartTime = DateTime.Now;

            //Set 5 minutes as time out for a process to finish (this is a sample)
            olease.LeaseTimeOut = olease.StartTime.AddMinutes(OrchestrationLeaseTimeOut);

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
            catch (Exception ex)
            {
                log.LogError("Error adquiring the orchestration lock:" + ex.Message);
                throw;
            }
        }


        private static async Task ReleaseLeaseAsync(ILogger log, Container container, OrchestrationLease olease)
        {
            ItemRequestOptions options = null;
            options = new ItemRequestOptions();
            options.IfMatchEtag = olease.ETag;

            if (olease.Locked)
            {
                olease.Locked = false;              //Reset lock   
                olease.EndTime = DateTime.Now;
                olease.LastLeaseTimedOut = false;   //Reset last lease timedout
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
            else
            {
                log.LogWarning("Lock is not adquired. Not releasing.");
            }
        }
    }
}