// See https://aka.ms/new-console-template for more information
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Xml.Linq;

string connectionString = "";
string containerName = "xml";
string tableName = "xmloutput";
string blobName = "Sample1.xml";
string partitionKey = "partitionKey";

// For blob Storage
var blobServiceClient = new BlobServiceClient(connectionString);
var blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);
var blob = blobContainerClient.GetBlockBlobClient(blobName);


// For table storage
CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
// Get a reference to the Azure table
CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
CloudTable table = tableClient.GetTableReference(tableName);

// Create a batch operation to insert the parsed entities into the table
TableBatchOperation batchOperation = new TableBatchOperation();

var propertyNames = new string[] { "Property1", "Property2", "Property3"};

using (Stream stream = await blob.OpenReadAsync())
{
    // Read entire xml file as a stream
    var xmlData = XDocument.Load(stream);

    // Get the EntityInformation element information
    var entityInformations = xmlData.Root?.Elements("Body").Descendants("Parent");

    if (entityInformations is null)
    {
        return;
    }

    foreach (var entityInfo in entityInformations)
    {
        var industryCode = entityInfo.Descendants(name: "SpecificCode").FirstOrDefault()?.Value;

        if (industryCode is null)
            return;

        // Step 1 : Create a new entity and insert it into the table
        var entity = new DynamicTableEntity(
            partitionKey: partitionKey,
            rowKey: industryCode);

        foreach (var propertyName in propertyNames)
        {
            entity.Properties[propertyName] = new EntityProperty(entityInfo.Descendants(name: propertyName).FirstOrDefault()?.Value);
        }

        // InsertOrMerge the table entity into the table.
        TableOperation insertOperation = TableOperation.InsertOrMerge(entity);
        await table.ExecuteAsync(insertOperation);
    }

}