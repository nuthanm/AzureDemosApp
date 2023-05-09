using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO.Compression;
using System.Linq.Expressions;
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
    private string originalFileName = string.Empty;
    private string nameOftheXmlFile = string.Empty;
    private Stream? xmlStream;
    private MemoryStream? ms;

    public async Task ReadBlobsFromContainerAsync()
    {
        try
        {
            var blobServiceClient = new BlobServiceClient(connectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient(containerName);


            Console.WriteLine("Process Begin.....");
            txtResult.AppendLine("Process Begin....");

            // Read all xml files from the container
            await foreach (BlobItem blobItem in containerClient.GetBlobsAsync())
            {
                // Get each blob from the container
                BlobClient blobClient = containerClient.GetBlobClient(blobItem.Name);
                BlobDownloadInfo downloadInfo = await blobClient.DownloadAsync();

                Console.WriteLine($"Original Blob name: {blobItem.Name}");
                txtResult.AppendLine($"Original Blob name: {blobItem.Name}");

                Console.WriteLine("----------");
                txtResult.AppendLine("-------------------");

                await UnzipAndCopyTheseStreamsInDestinationContainer(blobServiceClient, downloadInfo);
            }


            Console.WriteLine("Process Completed.");
            txtResult.AppendLine("Process Completed.");

            File.WriteAllText(Directory.GetCurrentDirectory() + @"/errorXML.txt", txtResult.ToString());

        }
        catch (Exception ex)
        {
            txtResult.AppendLine($"Error during processing zip/xml file : {ex.Message} and original fileName is {originalFileName}");
            Console.WriteLine($"Error during processing zip/xml file : {ex.Message} and original fileName is {originalFileName}");

            // copying the original file and place it in failded folder.
            await CopyBlobFromSourceToTarget(containerName, errContainer, originalFileName, false);
            File.WriteAllText(Directory.GetCurrentDirectory() + @"/errorXML.txt", txtResult.ToString());
            throw;
        }

    }

    private async Task UnzipAndCopyTheseStreamsInDestinationContainer(BlobServiceClient blobServiceClient, BlobDownloadInfo downloadInfo)
    {

        string outputFileName = string.Empty;
        int sequenceOfFile = 1;

        try
        {
            using (ZipArchive zipObj = new ZipArchive(downloadInfo.Content))
            {
                foreach (ZipArchiveEntry zipArchiveEntry in zipObj.Entries)
                {
                    Console.WriteLine($"Blob original fileName: {zipArchiveEntry.Name}, process begin");
                    txtResult.AppendLine($"Blob original fileName: {zipArchiveEntry.Name}, process begin");

                    // Get the contents of the entry
                    using (StreamReader reader = new StreamReader(zipArchiveEntry.Open()))
                    {
                        byte[] byteArray = Encoding.UTF8.GetBytes(reader.ReadToEnd());

                        var stream = new MemoryStream(byteArray);
                        var onlyFolder = zipArchiveEntry.FullName?.Contains('/') == true ? $"{zipArchiveEntry.FullName.Split('/')[0]}/" : string.Empty;
                        outputFileName = $"{onlyFolder}output_{DateTime.Now:yyyyMMddHHmmssfff}_{sequenceOfFile}.xml";
                        await PerformCopyAction(blobServiceClient, stream, outputFileName);
                    }

                    Console.WriteLine($"Blob updated fileName: {outputFileName} and file_{sequenceOfFile} out of {zipObj.Entries.Count}, process end");
                    txtResult.AppendLine($"Blob updated fileName: {outputFileName} and file_{sequenceOfFile} out of {zipObj.Entries.Count}, process end\"");
                    sequenceOfFile++;
                }
            }
        }
        catch
        {

            Console.WriteLine($"Error during unzip/copy action: {outputFileName}, process interrupt");
            txtResult.AppendLine($"Error during unzip/copy action: {outputFileName}, process interrupt");
            throw new Exception($"Error during unzip/copy action: {outputFileName}, process interrupt");
        }
    }

    private async Task PerformCopyAction(BlobServiceClient blobServiceClient, MemoryStream xmlStream, string outputFileName)
    {

        // Check if xmlStream is null then we stop whole process
        if (xmlStream is null)
        {
            txtResult.AppendLine($"No content in XML to process and fileName : {outputFileName}");
            throw new NullReferenceException($"No content in XML to process and fileName : {outputFileName}");
        }

        // We are adding new attribute in xml:noNamespaceSchemaLocation (If incase not there)
        var updatedStream = addXSINewAttribute(xmlStream);
        StreamReader reader = new StreamReader(updatedStream);
        string content = await reader.ReadToEndAsync();

        // Replace if any BOM characters in the xml
        content = Regex.Replace(content, @"\uFEFF", string.Empty);

        // Get xsd file from another container
        var xsdContainer = blobServiceClient.GetBlobContainerClient(strXsdContainer);
        var xsdBlobClient = xsdContainer.GetBlobClient("schema.xsd");

        // Checking whether xsd blob exists
        if (await xsdBlobClient.ExistsAsync())
        {
            var response = await xsdBlobClient.DownloadAsync();

            // Adding again XSI new attribute in xsd file
            updatedStream = GetUpdatedStreamForXSD(response);

            using (var streamReader = new StreamReader(updatedStream))
            {
                while (!streamReader.EndOfStream)
                {
                    var xsdContent = await streamReader.ReadToEndAsync();

                    // Option 1: Update the content in xml and place it in tempContainer
                    await UploadBlobContentIntoTemp(tempContainer, outputFileName, content);

                    // If you want to take the content from tempContainer then pass true or else false
                    await CopyBlobFromSourceToTarget(containerName, successContainer, outputFileName, true);
                }
            }
        }
        else
        {
            Console.WriteLine($"XSD Blob doesn't exit: {xsdBlobClient.Name}");
            txtResult.AppendLine($"XSD Blob doesn't exit: {xsdBlobClient.Name}");
        }
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

            // check blob exists in source
            if (!await sourceBlob.ExistsAsync())
            {
                txtResult.AppendLine($"Blob doesn't exist for copying data from source: {srcContainer} to target: {destinationContainer}");
                Console.WriteLine($"Blob doesn't exist for copying data from source: {srcContainer} to target: {destinationContainer}");
                return false;
            }

            // Copy - blob exists
            await targetBlob.StartCopyAsync(sourceBlob);

            // Delete the file from temp container
            if (isTemp)
                await sourceBlob.DeleteIfExistsAsync();

            txtResult.AppendLine($"Successfull - Blob copying data from source: {srcContainer} to target: {destinationContainer}");
            Console.WriteLine($"Successfull - Blob copying data from source: {srcContainer} to target: {destinationContainer}");
            resultResponse = true;
        }
        catch
        {
            resultResponse = false;
            txtResult.AppendLine($"Error during copying file from source: {srcContainer} to target: {destinationContainer}");
            Console.WriteLine($"Error during copying file from source: {srcContainer} to target: {destinationContainer}");
            throw;
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