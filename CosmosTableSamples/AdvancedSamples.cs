using System;
using System.Collections.Generic;

namespace CosmosTableSamples
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.Table;
    using Model;

    class AdvancedSamples
    {
        public async Task RunSamples()
        {
            Console.WriteLine("Azure Cosmos DB Table - Advanced Samples\n");
            Console.WriteLine();

            string tableName = "demo" + Guid.NewGuid().ToString().Substring(0, 5);

            // Create or reference an existing table
            CloudTable table = await Common.CreateTableAsync(tableName);
            CloudTableClient tableClient = table.ServiceClient;

            try
            {
                // Demonstrate advanced functionality such as batch operations and segmented multi-entity queries
                await AdvancedDataOperationsAsync(table);

                // List tables in the account
                await TableListingOperations(tableClient);
            }
            finally
            {
                // Delete the table
                await table.DeleteIfExistsAsync();
            }
        }

        private static async Task AdvancedDataOperationsAsync(CloudTable table)
        {
            // Demonstrate upsert and batch table operations
            Console.WriteLine("Inserting a batch of entities. ");
            await BatchInsertOfCustomerEntitiesAsync(table);
            Console.WriteLine();

            // Query a range of data within a partition using a simple query
            Console.WriteLine("Retrieving entities with surname of Smith and first names >= 1 and <= 75");
            ExecuteSimpleQuery(table, "Smith", "0001", "0075");
            Console.WriteLine();

            // Query the same range of data within a partition and return result segments of 50 entities at a time
            Console.WriteLine("Retrieving entities with surname of Smith and first names >= 1 and <= 75");
            await PartitionRangeQueryAsync(table, "Smith", "0001", "0075");
            Console.WriteLine();

            // Query for all the data within a partition 
            Console.WriteLine("Retrieve entities with surname of Smith.");
            await PartitionScanAsync(table, "Smith");
            Console.WriteLine();
        }

        /// <summary>
        /// List tables in the account.
        /// </summary>
        /// <param name="tableClient">The table client.</param>
        /// <returns>A Task object</returns>
        private static async Task TableListingOperations(CloudTableClient tableClient)
        {
            try
            {
                // To list all tables in the account, uncomment the following line.
                // Note that listing all tables in the account may take a long time if the account contains a large number of tables.
                // ListAllTables(tableClient);

                // List tables beginning with the specified prefix.
                await ListTablesWithPrefix(tableClient, "demo");
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// Lists tables in the account whose names begin with the specified prefix.
        /// </summary>
        /// <param name="tableClient">The Table service client object.</param>
        /// <param name="prefix">The table name prefix.</param>
        /// <returns>A Task object</returns>
        private static async Task ListTablesWithPrefix(CloudTableClient tableClient, string prefix)
        {
            Console.WriteLine("List all tables beginning with prefix {0}:", prefix);

            TableContinuationToken continuationToken = null;
            TableResultSegment resultSegment = null;

            try
            {
                do
                {
                    // List tables beginning with the specified prefix. 
                    // Passing in null for the maxResults parameter returns the maximum number of results (up to 5000).
                    resultSegment = await tableClient.ListTablesSegmentedAsync(
                        prefix, null, continuationToken, null, null);

                    // Enumerate the tables returned.
                    foreach (var table in resultSegment.Results)
                    {
                        Console.WriteLine("\tTable:" + table.Name);
                    }
                }
                while (continuationToken != null);
                Console.WriteLine();
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// Lists all tables in the account.
        /// </summary>
        /// <param name="tableClient">The Table service client object.</param>
        private static void ListAllTables(CloudTableClient tableClient)
        {
            Console.WriteLine("List all tables in account:");

            try
            {
                // Note that listing all tables in the account may take a long time if the account contains a large number of tables.
                foreach (var table in tableClient.ListTables())
                {
                    Console.WriteLine("\tTable:" + table.Name);
                }

                Console.WriteLine();
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// Demonstrate inserting of a large batch of entities. Some considerations for batch operations:
        ///  1. You can perform updates, deletes, and inserts in the same single batch operation.
        ///  2. A single batch operation can include up to 100 entities.
        ///  3. All entities in a single batch operation must have the same partition key.
        ///  4. While it is possible to perform a query as a batch operation, it must be the only operation in the batch.
        ///  5. Batch size must be less than or equal to 2 MB
        /// </summary>
        /// <param name="table">Sample table name</param>
        /// <returns>A Task object</returns>
        private static async Task BatchInsertOfCustomerEntitiesAsync(CloudTable table)
        {
            try
            {
                // Create the batch operation. 
                TableBatchOperation batchOperation = new TableBatchOperation();

                // The following code  generates test data for use during the query samples.  
                for (int i = 0; i < 100; i++)
                {
                    batchOperation.InsertOrMerge(new CustomerEntity("Smith", string.Format("{0}", i.ToString("D4")))
                    {
                        Email = string.Format("{0}@contoso.com", i.ToString("D4")),
                        PhoneNumber = string.Format("425-555-{0}", i.ToString("D4"))
                    });
                }

                // Execute the batch operation.
                IList<TableResult> results = await table.ExecuteBatchAsync(batchOperation);
                foreach (var res in results)
                {
                    var customerInserted = res.Result as CustomerEntity;
                    Console.WriteLine("Inserted entity with\t Etag = {0} and PartitionKey = {1}, RowKey = {2}", customerInserted.ETag, customerInserted.PartitionKey, customerInserted.RowKey);
                }
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// Demonstrate a partition range query that searches within a partition for a set of entities that are within a 
        /// specific range. This query returns all entities in the range. Note that if your table contains a large amount of data,
        /// the query may be slow or may time out. In that case, use a segmented query, as shown in the PartitionRangeQueryAsync() 
        /// sample method.
        /// Note that the ExecuteSimpleQuery method is called synchronously, for the purposes of the sample. However, in a real-world
        /// application using the async/await pattern, best practices recommend using asynchronous methods consistently.
        /// </summary>
        /// <param name="table">Sample table name</param>
        /// <param name="partitionKey">The partition within which to search</param>
        /// <param name="startRowKey">The lowest bound of the row key range within which to search</param>
        /// <param name="endRowKey">The highest bound of the row key range within which to search</param>
        private static void ExecuteSimpleQuery(CloudTable table, string partitionKey, string startRowKey,
            string endRowKey)
        {
            try
            {
                // Create the range query using the fluid API 
                TableQuery<CustomerEntity> rangeQuery = new TableQuery<CustomerEntity>().Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                        TableOperators.And,
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual,
                                startRowKey),
                            TableOperators.And,
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual,
                                endRowKey))));

                foreach (CustomerEntity entity in table.ExecuteQuery(rangeQuery))
                {
                    Console.WriteLine("Customer: {0},{1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey, entity.Email,
                        entity.PhoneNumber);
                }
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// Demonstrate a partition range query that searches within a partition for a set of entities that are within a 
        /// specific range. The async APIs require that the user handle the segment size and return the next segment 
        /// using continuation tokens. 
        /// </summary>
        /// <param name="table">Sample table name</param>
        /// <param name="partitionKey">The partition within which to search</param>
        /// <param name="startRowKey">The lowest bound of the row key range within which to search</param>
        /// <param name="endRowKey">The highest bound of the row key range within which to search</param>
        /// <returns>A Task object</returns>
        private static async Task PartitionRangeQueryAsync(CloudTable table, string partitionKey, string startRowKey, string endRowKey)
        {
            try
            {
                // Create the range query using the fluid API 
                TableQuery<CustomerEntity> rangeQuery = new TableQuery<CustomerEntity>().Where(
                    TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                            TableOperators.And,
                            TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, startRowKey),
                                TableOperators.And,
                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, endRowKey))));

                // Request 50 results at a time from the server. 
                TableContinuationToken token = null;
                rangeQuery.TakeCount = 50;
                int segmentNumber = 0;
                do
                {
                    // Execute the query, passing in the continuation token.
                    // The first time this method is called, the continuation token is null. If there are more results, the call
                    // populates the continuation token for use in the next call.
                    TableQuerySegment<CustomerEntity> segment = await table.ExecuteQuerySegmentedAsync(rangeQuery, token);

                    // Indicate which segment is being displayed
                    if (segment.Results.Count > 0)
                    {
                        segmentNumber++;
                        Console.WriteLine();
                        Console.WriteLine("Segment {0}", segmentNumber);
                    }

                    // Save the continuation token for the next call to ExecuteQuerySegmentedAsync
                    token = segment.ContinuationToken;

                    // Write out the properties for each entity returned.
                    foreach (CustomerEntity entity in segment)
                    {
                        Console.WriteLine("\t Customer: {0},{1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey, entity.Email, entity.PhoneNumber);
                    }

                    Console.WriteLine();
                }
                while (token != null);
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// Demonstrate a partition scan whereby we are searching for all the entities within a partition. Note this is not as efficient 
        /// as a range scan - but definitely more efficient than a full table scan. The async APIs require that the user handle the segment 
        /// size and return the next segment using continuation tokens.
        /// </summary>
        /// <param name="table">Sample table name</param>
        /// <param name="partitionKey">The partition within which to search</param>
        /// <returns>A Task object</returns>
        private static async Task PartitionScanAsync(CloudTable table, string partitionKey)
        {
            try
            {
                TableQuery<CustomerEntity> partitionScanQuery =
                    new TableQuery<CustomerEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

                TableContinuationToken token = null;

                // Read entities from each query segment.
                do
                {
                    TableQuerySegment<CustomerEntity> segment = await table.ExecuteQuerySegmentedAsync(partitionScanQuery, token);
                    token = segment.ContinuationToken;
                    foreach (CustomerEntity entity in segment)
                    {
                        Console.WriteLine("Customer: {0},{1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey, entity.Email, entity.PhoneNumber);
                    }
                }
                while (token != null);
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }
    }
}
