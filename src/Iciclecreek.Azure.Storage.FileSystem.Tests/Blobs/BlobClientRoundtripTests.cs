using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobClientRoundtripTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    [Test]
    public void Upload_Download_Roundtrip_Text()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "test-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("hello.txt");
        client.Upload(BinaryData.FromString("Hello, World!"));

        var downloaded = client.DownloadContent();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("Hello, World!"));
    }

    [Test]
    public void Upload_Download_Roundtrip_Binary()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "bin-container");
        container.CreateIfNotExists();

        var data = new byte[] { 0, 1, 2, 3, 255, 254, 253, 252 };
        BlobClient client = container.GetBlobClient("data.bin");
        client.Upload(new BinaryData(data));

        var downloaded = client.DownloadContent();
        Assert.That(downloaded.Value.Content.ToArray(), Is.EqualTo(data));
    }

    [Test]
    public void Upload_Creates_Real_File_On_Disk()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "disk-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("real-file.txt");
        client.Upload(BinaryData.FromString("filesystem test"));

        var expectedPath = Path.Combine(_root.Account.BlobsRootPath, "disk-container", "real-file.txt");
        Assert.That(File.Exists(expectedPath), Is.True);
        Assert.That(File.ReadAllText(expectedPath), Is.EqualTo("filesystem test"));
    }

    [Test]
    public void Exists_Returns_False_For_Missing_Blob()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "exists-test");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("nope.txt");
        Assert.That(client.Exists().Value, Is.False);
    }

    [Test]
    public void Exists_Returns_True_After_Upload()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "exists-test");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("yes.txt");
        client.Upload(BinaryData.FromString("data"));
        Assert.That(client.Exists().Value, Is.True);
    }

    [Test]
    public void Delete_Removes_Blob()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "del-test");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("del.txt");
        client.Upload(BinaryData.FromString("bye"));
        Assert.That(client.Exists().Value, Is.True);

        client.Delete();
        Assert.That(client.Exists().Value, Is.False);
    }

    [Test]
    public void GetProperties_Returns_ContentType_And_Length()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "props-test");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("props.txt");
        client.Upload(BinaryData.FromString("content"), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = "text/plain" }
        });

        var props = client.GetProperties().Value;
        Assert.That(props.ContentLength, Is.EqualTo(7));
        Assert.That(props.ContentType, Is.EqualTo("text/plain"));
        Assert.That(props.ETag.ToString(), Does.StartWith("\"0x"));
    }

    [Test]
    public void SetMetadata_Persists()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "meta-test");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("meta.txt");
        client.Upload(BinaryData.FromString("x"));

        client.SetMetadata(new Dictionary<string, string> { ["color"] = "blue" });
        var props = client.GetProperties().Value;
        Assert.That(props.Metadata["color"], Is.EqualTo("blue"));
    }

    [Test]
    public async Task Upload_Download_Async_Works()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "async-test");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("async.txt");
        await client.UploadAsync(BinaryData.FromString("async data"));

        var downloaded = await client.DownloadContentAsync();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("async data"));
    }

    [Test]
    public async Task Upload_Download_Roundtrip_Text_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "test-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("hello.txt");
        await client.UploadAsync(BinaryData.FromString("Hello, World!"));

        var downloaded = await client.DownloadContentAsync();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("Hello, World!"));
    }

    [Test]
    public async Task Upload_Download_Roundtrip_Binary_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "bin-container");
        await container.CreateIfNotExistsAsync();

        var data = new byte[] { 0, 1, 2, 3, 255, 254, 253, 252 };
        BlobClient client = container.GetBlobClient("data.bin");
        await client.UploadAsync(new BinaryData(data));

        var downloaded = await client.DownloadContentAsync();
        Assert.That(downloaded.Value.Content.ToArray(), Is.EqualTo(data));
    }

    [Test]
    public async Task Upload_Creates_Real_File_On_Disk_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "disk-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("real-file.txt");
        await client.UploadAsync(BinaryData.FromString("filesystem test"));

        var expectedPath = Path.Combine(_root.Account.BlobsRootPath, "disk-container", "real-file.txt");
        Assert.That(File.Exists(expectedPath), Is.True);
        Assert.That(File.ReadAllText(expectedPath), Is.EqualTo("filesystem test"));
    }

    [Test]
    public async Task Exists_Returns_False_For_Missing_Blob_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "exists-test");
        await container.CreateIfNotExistsAsync();

        var client = container.GetBlobClient("nope.txt");
        Assert.That((await client.ExistsAsync()).Value, Is.False);
    }

    [Test]
    public async Task Exists_Returns_True_After_Upload_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "exists-test");
        await container.CreateIfNotExistsAsync();

        var client = container.GetBlobClient("yes.txt");
        await client.UploadAsync(BinaryData.FromString("data"));
        Assert.That((await client.ExistsAsync()).Value, Is.True);
    }

    [Test]
    public async Task Delete_Removes_Blob_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "del-test");
        await container.CreateIfNotExistsAsync();

        var client = container.GetBlobClient("del.txt");
        await client.UploadAsync(BinaryData.FromString("bye"));
        Assert.That((await client.ExistsAsync()).Value, Is.True);

        await client.DeleteAsync();
        Assert.That((await client.ExistsAsync()).Value, Is.False);
    }

    [Test]
    public async Task GetProperties_Returns_ContentType_And_Length_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "props-test");
        await container.CreateIfNotExistsAsync();

        var client = container.GetBlobClient("props.txt");
        await client.UploadAsync(BinaryData.FromString("content"), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = "text/plain" }
        });

        var props = (await client.GetPropertiesAsync()).Value;
        Assert.That(props.ContentLength, Is.EqualTo(7));
        Assert.That(props.ContentType, Is.EqualTo("text/plain"));
        Assert.That(props.ETag.ToString(), Does.StartWith("\"0x"));
    }

    [Test]
    public async Task SetMetadata_Persists_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "meta-test");
        await container.CreateIfNotExistsAsync();

        var client = container.GetBlobClient("meta.txt");
        await client.UploadAsync(BinaryData.FromString("x"));

        await client.SetMetadataAsync(new Dictionary<string, string> { ["color"] = "blue" });
        var props = (await client.GetPropertiesAsync()).Value;
        Assert.That(props.Metadata["color"], Is.EqualTo("blue"));
    }
}
