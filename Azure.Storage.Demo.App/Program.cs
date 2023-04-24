// See https://aka.ms/new-console-template for more information
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Text.Json;
using System.Xml.Linq;

string connectionString = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";
string containerName = "xml";
string tableName = "xmloutput";
string blobName = "Sample.xml";

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

var propertyNames = new string[] { "PreferredName", "IndustryCode", "AddressLine1", "CityName", "StateOrProvince", "PostalCountry" };

using (Stream stream = await blob.OpenReadAsync())
{
    var xmlData = XDocument.Load(stream); 

    foreach (var propertyName in propertyNames)
    {
        var elementValues = xmlData.Descendants(propertyName);

        if (elementValues.Any())
        {
            // Create a list to hold the property values.
            List<string> propertyValues = new List<string>();

            foreach (var element in elementValues)
            {
                propertyValues.Add(element.Value);
            }

            // Create a new entity and insert it into the table
            var entity = new DynamicTableEntity(
                partitionKey: blobName,
                rowKey: "IndustryCode");

            string jsonPropertyValue = JsonSerializer.Serialize(propertyValues);

            entity.Properties[propertyName] = new EntityProperty(jsonPropertyValue);

            // InsertOrMerge the table entity into the table.
            TableOperation insertOperation = TableOperation.InsertOrMerge(entity);
            await table.ExecuteAsync(insertOperation);
        }
    }
}

