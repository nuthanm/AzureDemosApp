//using System.Xml;
//using System.Xml.Schema;
//using Azure.Storage.Blobs.Specialized;
//using Azure.Storage.Blobs.Models;
//using Azure.Storage.Blobs;
//using System.Text;
//using static System.Net.Mime.MediaTypeNames;
//using System.Reflection.Metadata;
//using System.Xml.Linq;
//using Microsoft.WindowsAzure.Storage;
//using Microsoft.WindowsAzure.Storage.Blob;
//using System.IO;

//await new ConsoleApplication3().ReadBlobsFromContainerAsync();


//public class ConsoleApplication3
//{
//    public StringBuilder txtResult = new StringBuilder();




//    public string connectionString = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";
//    public string containerName = "zipoutput";
//    public string successContainer = "validationpassed";
//    public string errContainer = "failed";
//    public string tableName = "aggatewayencoding";
//    string partitionKey = "entityData";

//    public async Task ReadBlobsFromContainerAsync()
//    {

//        BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
//        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);


//        await foreach (BlobItem blobItem in containerClient.GetBlobsAsync())
//        {

//            BlobClient blobClient = containerClient.GetBlobClient(blobItem.Name);
//            BlobDownloadInfo downloadInfo = await blobClient.DownloadAsync();

//            XmlDocument? doc = new XmlDocument();
//            doc.Load(downloadInfo.Content);

//            // Get the root element of the XML file
//            XmlElement? root = doc?.DocumentElement;

//            // Check if the xsi:noNamespaceSchemaLocation attribute is already present in the XML file
//            if (!root.HasAttribute("xsi:noNamespaceSchemaLocation"))
//            {
//                // Add the xsi:noNamespaceSchemaLocation attribute to the root element
//                root.SetAttribute("xsi:noNamespaceSchemaLocation", "abc.xsd");
//            }

//            doc?.Save("C:\\Users\\Administrator\\Desktop\\temp\\dynamic.xml");


//            using StreamReader reader = new StreamReader(downloadInfo.Content);
//            string content = await reader.ReadToEndAsync();

//            // check whether xml having bom encoded or not
//            bool isXMLWithBOM = IsXMLFilePresentWithUTF8BOM(content);

//            // if file with BOM then we convert from BOM to normal
//            if (isXMLWithBOM)
//            {
//                // Convert this to UTF8
//                // Remove the BOM from the input content by creating a new UTF-8 encoding
//                // this instance doesn't include the BOM in the outputContent

//                var utf8WithoutBom = new UTF8Encoding(false);
//                var encodedBytes = utf8WithoutBom.GetBytes(content);
//                var outputContent = utf8WithoutBom.GetString(encodedBytes);

//                // validate again for BOM checking with new content
//                // isXMLWithBOM = IsXMLFilePresentWithUTF8BOM(outputContent);

//                content = outputContent; // updated with new content
//            }

//            bool xmlResponse = ValidateXml(content, "C:\\Users\\Administrator\\Desktop\\temp\\schemaBOM.xsd");

//            if (xmlResponse)
//            {
//                await Move(containerName, successContainer, blobItem.Name);
//            }
//            else
//            {
//                await Move(containerName, errContainer, blobItem.Name);
//            }

//            Console.WriteLine($"Blob name: {blobItem.Name}");
//            txtResult.AppendLine($"Blob name: {blobItem.Name}");

//            Console.WriteLine("----------");
//            File.WriteAllText(Directory.GetCurrentDirectory() + @"/errorXML.txt", txtResult.ToString());
//            BlobContainerClient xmlContainer = blobServiceClient.GetBlobContainerClient(successContainer);
//        }
//    }


//    public bool ValidateXml(string xmlString, string xsdFilePath)
//    {
//        bool success;
//        // Configure the XmlReaderSettings
//        XmlReaderSettings settings = new XmlReaderSettings();
//        settings.ValidationType = ValidationType.Schema;
//        settings.ValidationFlags |= XmlSchemaValidationFlags.ProcessInlineSchema;
//        settings.ValidationFlags |= XmlSchemaValidationFlags.ProcessSchemaLocation;
//        settings.ValidationFlags |= XmlSchemaValidationFlags.ReportValidationWarnings;
//        settings.ValidationEventHandler += new ValidationEventHandler(ValidationEventHandler);

//        // Add the XSD schema to the settings
//        settings.Schemas.Add(null, XmlReader.Create(xsdFilePath));


//        // Create the XmlReader and validate the XML
//        using (StringReader stringReader = new StringReader(xmlString))
//        {
//            using (XmlReader reader = XmlReader.Create(stringReader, settings))
//            {
//                try
//                {
//                    while (reader.Read()) { }
//                    //Console.WriteLine("The XML string is valid according to the XSD schema.");
//                    txtResult.AppendLine("The XML string is valid according to the XSD schema.");
//                    success = true;
//                }
//                catch (XmlException ex)
//                {
//                    Console.WriteLine($"An XML error occurred: {ex.Message}");
//                    txtResult.AppendLine($"An XML error occurred: {ex.Message}");
//                    success = false;
//                }
//                catch (Exception ex)
//                {
//                    Console.WriteLine($"A non-XML error occurred: {ex.Message}");
//                    txtResult.AppendLine($"An XML error occurred: {ex.Message}");
//                    success = false;
//                }
//                return success;
//            }
//        }
//    }


//    public static void ValidationEventHandler(object sender, ValidationEventArgs e)
//    {
//        if (e.Severity == XmlSeverityType.Warning)
//        {
//            Console.WriteLine($"Warning: {e.Message}");
//        }
//        else if (e.Severity == XmlSeverityType.Error)
//        {
//            Console.WriteLine($"Error: {e.Message}");
//        }
//    }

//    public async Task<bool> Move(string srcContainer, string destinationContainer, string srcFileName)
//    {
//        var storageAccount = CloudStorageAccount.Parse(connectionString);
//        CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
//        CloudBlobContainer sourceContainer = blobClient.GetContainerReference(srcContainer);
//        CloudBlobContainer targetContainer = blobClient.GetContainerReference(destinationContainer);

//        CloudBlockBlob sourceBlob = sourceContainer.GetBlockBlobReference(srcFileName);
//        CloudBlockBlob targetBlob = targetContainer.GetBlockBlobReference(srcFileName);
//        await targetBlob.StartCopyAsync(sourceBlob);
//        return await Task.FromResult(true);

//    }

//    public bool IsXMLFilePresentWithUTF8BOM(string inputString)
//    {
//        bool isXMLFilePresentWithUTF8BOM = false;

//        byte[] byteArray = Encoding.UTF8.GetBytes(inputString);

//        if (byteArray.Length >= 3 && byteArray[0] == 0xEF && byteArray[1] == 0xBB && byteArray[2] == 0xBF)
//        {
//            isXMLFilePresentWithUTF8BOM = true;
//        }

//        return isXMLFilePresentWithUTF8BOM;
//    }



//}

using System.Xml;
using System.Xml.Schema;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Text;
using static System.Net.Mime.MediaTypeNames;
using Microsoft.WindowsAzure.Storage.Table;
using System.Reflection.Metadata;
using System.Xml.Linq;
using System.Text.RegularExpressions;
using System.ComponentModel;
using Newtonsoft.Json.Linq;
using System.Net.NetworkInformation;
using System;

await new ConsoleApplication3().ReadBlobsFromContainerAsync();


public class ConsoleApplication3
{
    public StringBuilder txtResult = new StringBuilder();



    public string connectionString = "\"AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;\";";
    public string containerName = "unzippedxmladf";
    public string xsdContainer = "xsdcontainer-aggateway";
    public string successContainer = "xmlvalidationupdateschema";
    public string errContainer = "errorxml";
    public string tableName = "aggatewayruntabledemo";
    string partitionKey = "entityData";

    public async Task ReadBlobsFromContainerAsync()
    {

        BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);




        await foreach (BlobItem blobItem in containerClient.GetBlobsAsync())
        {

            BlobClient blobClient = containerClient.GetBlobClient(blobItem.Name);


            BlobDownloadInfo downloadInfo = await blobClient.DownloadAsync();

            XmlDocument? doc = new XmlDocument();
            doc.Load(downloadInfo.Content);
            XmlElement? root = doc?.DocumentElement;
            if (!root.HasAttribute("xsi:noNamespaceSchemaLocation"))
            {
                root.SetAttribute("xsi.noNamespaceShemaLocation", "aggateway.xsd");
            }




            content = Regex.Replace(content, @"\uFEFF", string.Empty);
            BlobServiceClient xsdBlob = new BlobServiceClient("connectionString");
            BlobContainerClient xsdContainer = blobServiceClient.GetBlobContainerClient("xsdContainer");
            BlobClient xsdBlobClient = containerClient.GetBlobClient("aggateway.xsd");
            if (await xsdBlobClient.ExistsAsync())
            {
                var response = await xsdBlobClient.DownloadAsync();
                using (var streamReader = new StreamReader(response.Value.Content))
                {
                    while (!streamReader.EndOfStream)
                    {
                        var line = await streamReader.ReadLineAsync();
                        Console.WriteLine(line);
                        bool xmlResponse = ValidateXml(content, line);

                        if (xmlResponse)
                        {
                            await Move(containerName, successContainer, blobItem.Name);
                        }
                        else
                        {
                            await Move(containerName, errContainer, blobItem.Name);
                        }
                    }
                }
            }

            Console.WriteLine($"Blob name: {blobItem.Name}");
            txtResult.AppendLine($"Blob name: {blobItem.Name}");
            Console.WriteLine("----------");
            File.WriteAllText(Directory.GetCurrentDirectory() + @"/errorXML.txt", txtResult.ToString());
            BlobContainerClient xmlContainer = blobServiceClient.GetBlobContainerClient(successContainer);
            BlobClient xmlParsedBlob = xmlContainer.GetBlobClient(blobItem.Name);
            using (Stream stream = await xmlParsedBlob.OpenReadAsync())
            {
                var xmlData = XDocument.Load(stream);

            }

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
        settings.Schemas.Add(null, XmlReader.Create(xsdFilePath));


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

    public async Task<bool> Move(string srcContainer, string destinationContainer, string srcFileName)
    {
        var storageAccount = CloudStorageAccount.Parse(connectionString);
        CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
        CloudBlobContainer sourceContainer = blobClient.GetContainerReference(srcContainer);
        CloudBlobContainer targetContainer = blobClient.GetContainerReference(destinationContainer);


        CloudBlockBlob sourceBlob = sourceContainer.GetBlockBlobReference(srcFileName);
        CloudBlockBlob targetBlob = targetContainer.GetBlockBlobReference(srcFileName);
        await targetBlob.StartCopyAsync(sourceBlob);
        return await Task.FromResult(true);

    }


}