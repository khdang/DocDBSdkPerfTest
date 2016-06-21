using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Net;
using SdkPerfTest.Helpers;
namespace SdkPerfTest
{
    class DocDBHelper
    {
        private static readonly string endpointUrl = ConfigurationManager.AppSettings["EndPointUrl"];
        private static readonly string authorizationKey = ConfigurationManager.AppSettings["AuthorizationKey"];
        private static readonly string databaseName = ConfigurationManager.AppSettings["DatabaseId"];
        private static readonly string collectionName = ConfigurationManager.AppSettings["CollectionId"];

        private static readonly Random rand = new Random(DateTime.Now.Millisecond);

        internal const int MaxRUPerPartition = 10000;
        internal static int PartitionCount = 2;
        internal static int OfferThroughput = PartitionCount * MaxRUPerPartition;
        public static bool ReCreateDatabase = true;

        public static int MaxRetryTimes = 0;

        //Reusable instance of DocumentClient which represents the connection to a DocumentDB endpoint
        private static DocumentClient client;

        internal static async Task ReadDocumentAsync(string documentId, string accountNumber)
        {
            try
            {
                ResourceResponse<Document> response;
                if (DocDBHelper.PartitionCount > 1)
                {
                    response = await client.ReadDocumentAsync(UriFactory.CreateDocumentUri(databaseName, collectionName, documentId), 
                        new RequestOptions { PartitionKey = new PartitionKey(accountNumber) }
                    );
                }
                else
                {
                    response = await client.ReadDocumentAsync(UriFactory.CreateDocumentUri(databaseName, collectionName, documentId));
                }
                

                SalesOrder readOrder = (SalesOrder)(dynamic)response.Resource;

                if (Program.useThroughputHelper)
                {
                    ThroughputHelper.GetInstance().RequestComplete(response.RequestCharge, (int)response.StatusCode);
                }
                if (Program.useSimpleThroughputHelper)
                {
                    SimpleThroughputHelper.GetInstance().AddEntry(Convert.ToInt64(response.RequestCharge));
                }
            }
            catch (AggregateException ex)
            {
                //var dce = ex.InnerException as DocumentClientException;
                //if (dce == null) { throw ex; }

                //if (Program.useThroughputHelper)
                //{
                //    ThroughputHelper.GetInstance().RequestComplete(dce.RequestCharge, (int)dce.StatusCode);
                //}
                //if (Program.useSimpleThroughputHelper)
                //{
                //    SimpleThroughputHelper.GetInstance().AddEntry(Convert.ToInt64(dce.RequestCharge));
                //}
            }
        }

        internal static async Task RunOneCycle()
        {
            //var sw = Stopwatch.StartNew();
            //try
            //{
                var documentId = string.Format("{0}{1}", DateTime.Now.Ticks, Thread.CurrentThread.GetHashCode());
                var accountNumber = rand.Next(PartitionCount);

                await CreateDocumentsAsync(documentId, accountNumber.ToString());

                await ReadDocumentAsync(documentId, accountNumber.ToString());
            //}
            //catch (Exception ex)
            //{

            //}
            //finally
            //{
                //sw.Stop();
                //Console.WriteLine("RunOneCyle() completed in {0:2} ms at {1}", sw.ElapsedMilliseconds, DateTime.Now.Ticks);
            //}
        }

        internal static async Task Initialize()
        {
            client = new DocumentClient(new Uri(endpointUrl), authorizationKey);

            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = MaxRetryTimes;

            if (ReCreateDatabase)
            {
                await DeleteDatabaseIfExists(databaseName);

                await client.CreateDatabaseAsync(new Database { Id = databaseName });

                // We create a partitioned collection here which needs a partition key. Partitioned collections
                // can be created with very high values of provisioned throughput (up to OfferThroughput = 250,000)
                // and used to store up to 250 GB of data. You can also skip specifying a partition key to create
                // single partition collections that store up to 10 GB of data.
                DocumentCollection collectionDefinition = new DocumentCollection();
                collectionDefinition.Id = collectionName;

                if (DocDBHelper.PartitionCount > 1)
                {
                    // For this demo, we create a collection to store SalesOrders. We set the partition key to the account
                    // number so that we can retrieve all sales orders for an account efficiently from a single partition,
                    // and perform transactions across multiple sales order for a single account number. 
                    collectionDefinition.PartitionKey.Paths.Add("/AccountNumber");
                }

                // Use the recommended indexing policy which supports range queries/sorting on strings
                collectionDefinition.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });

                // Create with a throughput of 1000 RU/s
                await client.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(databaseName),
                    collectionDefinition,
                    new RequestOptions { OfferThroughput = DocDBHelper.OfferThroughput });
            }
        }

        internal static async Task<Database> DeleteDatabaseIfExists(string databaseId)
        {
            var databaseUri = UriFactory.CreateDatabaseUri(databaseId);

            Database database = client.CreateDatabaseQuery()
                .Where(db => db.Id == databaseId)
                .ToArray()
                .FirstOrDefault();

            if (database != null)
            {
                await client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(databaseId));
            }

            return database;
        }

        internal static async Task CreateDocumentsAsync(string documentId, string accountNumber)
        {
            try
            {
                Uri collectionLink = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName);

                SalesOrder salesOrder = SalesOrder.GetSalesOrderSample(documentId, accountNumber);
                var response = await client.CreateDocumentAsync(collectionLink, salesOrder);

                if (Program.useThroughputHelper)
                {
                    ThroughputHelper.GetInstance().RequestComplete(response.RequestCharge, (int)response.StatusCode);
                }
                if (Program.useSimpleThroughputHelper)
                {
                    SimpleThroughputHelper.GetInstance().AddEntry(Convert.ToInt64(response.RequestCharge));
                }
            }
            catch (AggregateException ex)
            {
                //var dce = ex.InnerException as DocumentClientException;
                //if (dce == null) { throw ex; }

                //if (Program.useThroughputHelper)
                //{
                //    ThroughputHelper.GetInstance().RequestComplete(dce.RequestCharge, (int)dce.StatusCode);
                //}
                //if (Program.useSimpleThroughputHelper)
                //{
                //    SimpleThroughputHelper.GetInstance().AddEntry(Convert.ToInt64(dce.RequestCharge));
                //}
            }
        }

        internal static void Cleanup()
        {
            client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(databaseName)).Wait();

            client.Dispose();
        }
    }
}
