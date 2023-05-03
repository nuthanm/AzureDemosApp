using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.ComponentModel;
using System.IO.Compression;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Schema;

await new ConsoleApplication3().ReadBlobsFromContainerAsync();


public class ConsoleApplication3
{
    public StringBuilder txtResult = new StringBuilder();


    public string connectionString = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";
    public string containerName = "zipoutput";
    public string successContainer = "validationpassed";
    public string errContainer = "failed";
    public string tableName = "aggatewayencoding";
    string partitionKey = "entityData";
    public string strXsdContainer = "xsdcontainer-aggateway";
    private string tempContainer = "temp";


    public async Task ReadBlobsFromContainerAsync()
    {
        var blobServiceClient = new BlobServiceClient(connectionString);
        var containerClient = blobServiceClient.GetBlobContainerClient(containerName);

        // Read all xml files from the container
        await foreach (BlobItem blobItem in containerClient.GetBlobsAsync())
        {
            // Get each blob from the container
            BlobClient blobClient = containerClient.GetBlobClient(blobItem.Name);
            BlobDownloadInfo downloadInfo = await blobClient.DownloadAsync();

            // get xml from zip file
            var (xmlStream, nameOftheXmlFile) = GetXMLinStream(downloadInfo);

            if (xmlStream is null)
                throw new NullReferenceException("No content in xml");

            // We are adding new attribute in xml:noNamespaceSchemaLocation (If incase not there)
            var updatedStream = addXSINewAttribute(xmlStream);

            // Pass the updatedStream for further operation
            using StreamReader reader = new StreamReader(updatedStream);
            string content = await reader.ReadToEndAsync();

            // Replace if any BOM characters in the xml
            content = Regex.Replace(content, @"\uFEFF", string.Empty);

            // Get xsd file from another container
            var xsdContainer = blobServiceClient.GetBlobContainerClient(strXsdContainer);
            var xsdBlobClient = xsdContainer.GetBlobClient("schema.xsd");

            // Checking whether blob exists
            if (await xsdBlobClient.ExistsAsync())
            {
                var response = await xsdBlobClient.DownloadAsync();

                // Adding again XSI new attribute in xsd file
                updatedStream = GetUpdatedStreamForXSD(response);

                using (var streamReader = new StreamReader(updatedStream))
                {
                    while (!streamReader.EndOfStream)
                    {
                        var line = await streamReader.ReadToEndAsync();

                        // Validate this xml
                        bool xmlResponse = ValidateXml(content, line);

                        // Option 1: Update the content in source place
                        await UploadBlobContentIntoTemp(tempContainer, nameOftheXmlFile, content);

                        if (xmlResponse)
                        {
                            // If you want to take the content from temp then pass true or else false
                            await CopyBlobFromSourceToTarget(containerName, successContainer, nameOftheXmlFile, true);
                            // await CopyBlobFromSourceToTarget(containerName, successContainer, blobItem.Name, true);
                        }
                        else
                        {
                            await CopyBlobFromSourceToTarget(containerName, errContainer, nameOftheXmlFile, true);
                        }
                    }
                }
            }

            Console.WriteLine($"Blob name: {blobItem.Name}");
            txtResult.AppendLine($"Blob name: {blobItem.Name}");
            Console.WriteLine("----------");
            File.WriteAllText(Directory.GetCurrentDirectory() + @"/errorXML.txt", txtResult.ToString());
            BlobContainerClient xmlContainer = blobServiceClient.GetBlobContainerClient(successContainer);
            BlobClient xmlParsedBlob = xmlContainer.GetBlobClient(nameOftheXmlFile);
            using (Stream stream = await xmlParsedBlob.OpenReadAsync())
            {
                var xmlData = XDocument.Load(stream);
            }
        }
    }

    private (Stream?, string) GetXMLinStream(BlobDownloadInfo downloadInfo)
    {
        using (ZipArchive zipObj = new ZipArchive(downloadInfo.Content))
        {
            foreach (ZipArchiveEntry zipArchiveEntry in zipObj.Entries)
            {
                // Get the contents of the entry
                using (StreamReader reader = new StreamReader(zipArchiveEntry.Open()))
                {
                    byte[] byteArray = Encoding.UTF8.GetBytes(reader.ReadToEnd());

                    return (new MemoryStream(byteArray), zipArchiveEntry.Name);
                }
            }
        }

        return (null, null);
    }

    public bool ValidateXml(string xmlString, string xsdFilePath)
    {
        bool success;

        // Configure the XmlReaderSettings
        XmlReaderSettings settings = new XmlReaderSettings();
        settings.ValidationType = ValidationType.Schema;
        settings.ValidationFlags |= XmlSchemaValidationFlags.ProcessInlineSchema;
        settings.ValidationFlags |= XmlSchemaValidationFlags.ProcessSchemaLocation;
        settings.ValidationFlags |= XmlSchemaValidationFlags.ReportValidationWarnings;
        settings.ValidationEventHandler += new ValidationEventHandler(ValidationEventHandler);

        // Add the XSD schema to the settings
        //settings.Schemas.Add(null, XmlReader.Create(xsdFilePath));

        // Create the XmlReader and validate the XML
        using (StringReader stringReader = new StringReader(xmlString))
        {
            using (XmlReader reader = XmlReader.Create(stringReader, settings))
            {
                try
                {
                    while (reader.Read()) { }
                    Console.WriteLine("The XML string is valid according to the XSD schema.");
                    txtResult.AppendLine("The XML string is valid according to the XSD schema.");
                    success = true;
                }
                catch (XmlException ex)
                {
                    Console.WriteLine($"An XML error occurred: {ex.Message}");
                    txtResult.AppendLine($"An XML error occurred: {ex.Message}");
                    success = false;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"A non-XML error occurred: {ex.Message}");
                    txtResult.AppendLine($"An XML error occurred: {ex.Message}");
                    success = false;
                }
                return success;

            }
        }
    }

    public static void ValidationEventHandler(object sender, ValidationEventArgs e)
    {
        if (e.Severity == XmlSeverityType.Warning)
        {
            Console.WriteLine($"Warning: {e.Message}");
        }
        else if (e.Severity == XmlSeverityType.Error)
        {
            Console.WriteLine($"Error: {e.Message}");
        }
    }

    private async Task<bool> UploadBlobContentIntoTemp(string containerName, string fileName, string content)
    {
        var returnStatus = false;

        try
        {
            // Source
            var containerClient = GetContainerReference(containerName);
            var blobClient = containerClient.GetBlobClient(fileName);

            // create container if not exists
            await containerClient.CreateIfNotExistsAsync();

            // first delete the blob and then upload the updated content
            await blobClient.DeleteIfExistsAsync();

            // Convert the content string to a byte array
            byte[] byteArray = Encoding.UTF8.GetBytes(content);

            // Upload the content to the blob
            using (var stream = new MemoryStream(byteArray))
            {
                await blobClient.UploadAsync(stream);
            }
            returnStatus = true;
        }
        catch
        {
            returnStatus = false;
        }
        return await Task.FromResult(returnStatus);
    }

    private BlobContainerClient GetContainerReference(string containerName)
    {
        var blobServiceClient = new BlobServiceClient(connectionString);
        return blobServiceClient.GetBlobContainerClient(containerName);
    }

    private async Task<bool> CopyBlobFromSourceToTarget(string srcContainer, string destinationContainer, string srcFileName, bool isTemp = false)
    {
        var resultResponse = false;
        srcContainer = isTemp ? tempContainer : srcContainer;

        try
        {
            // Getting References
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer sourceContainer = blobClient.GetContainerReference(srcContainer);
            CloudBlobContainer targetContainer = blobClient.GetContainerReference(destinationContainer);
            CloudBlockBlob sourceBlob = sourceContainer.GetBlockBlobReference(srcFileName);
            CloudBlockBlob targetBlob = targetContainer.GetBlockBlobReference(srcFileName);

            // Copy 
            await targetBlob.StartCopyAsync(sourceBlob);

            // Delete the file from temp container
            if (isTemp)
                await sourceContainer.DeleteIfExistsAsync();

            resultResponse = true;
        }
        catch
        {
            resultResponse = false;
        }

        return await Task.FromResult(resultResponse);
    }

    private static MemoryStream addXSINewAttribute(Stream downloadInfo)
    {
        var updatedStream = new MemoryStream();
        XmlDocument? doc = new XmlDocument();
        doc.Load(downloadInfo);

        XmlElement? root = doc?.DocumentElement;
        XmlNamespaceManager? nmMgr = new XmlNamespaceManager(doc.NameTable);
        nmMgr.AddNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");

        if (!root.HasAttribute("xsi:noNamespaceSchemaLocation"))
        {
            root.SetAttribute("noNamespaceShemaLocation", "http://www.w3.org/2001/XMLSchema-instance", "aggateway.xsd");
        }

        doc?.Save(updatedStream);
        updatedStream.Position = 0;
        return updatedStream;
    }
    private MemoryStream GetUpdatedStreamForXSD(BlobDownloadInfo downloadInfo)
    {
        var updatedStream = new MemoryStream();
        XmlDocument? doc = new XmlDocument();
        doc.Load(downloadInfo.Content);

        XmlElement? root = doc?.DocumentElement;
        XmlNamespaceManager? nmMgr = new XmlNamespaceManager(doc.NameTable);
        nmMgr.AddNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");


        if (!root.HasAttribute("xsi:noNamespaceSchemaLocation", "http://www.w3.org/2001/XMLSchema-instance"))
        {
            XmlAttribute attribute = root.OwnerDocument.CreateAttribute("xsi", "noNamespaceSchemaLocation", "http://www.w3.org/2001/XMLSchema-instance");
            attribute.Value = "abc.xsd";
            root.SetAttributeNode(attribute);
        }

        doc?.Save(updatedStream);
        updatedStream.Position = 0;
        return updatedStream;
    }
    #region Ignore
    public bool IsXMLFilePresentWithUTF8BOM(string inputString)
    {
        bool isXMLFilePresentWithUTF8BOM = false;

        byte[] byteArray = Encoding.UTF8.GetBytes(inputString);

        if (byteArray.Length >= 3 && byteArray[0] == 0xEF && byteArray[1] == 0xBB && byteArray[2] == 0xBF)
        {
            isXMLFilePresentWithUTF8BOM = true;
        }

        return isXMLFilePresentWithUTF8BOM;
    }
    #endregion Ignore

}