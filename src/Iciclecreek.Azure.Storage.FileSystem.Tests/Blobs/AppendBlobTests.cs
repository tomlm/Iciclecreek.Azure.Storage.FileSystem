using Azure;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class AppendBlobTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    [Test]
    public void Create_And_AppendBlock_Roundtrip()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "append");
        container.CreateIfNotExists();

        var client = container.GetFileAppendBlobClient("log.txt");
        client.Create(new AppendBlobCreateOptions());

        client.AppendBlock(new MemoryStream("line 1\n"u8.ToArray()));
        client.AppendBlock(new MemoryStream("line 2\n"u8.ToArray()));

        var content = client.DownloadContent().Value.Content.ToString();
        Assert.That(content, Is.EqualTo("line 1\nline 2\n"));
    }

    [Test]
    public void Create_Sets_BlobType_Append()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "appendtype");
        container.CreateIfNotExists();

        var client = container.GetFileAppendBlobClient("typed.txt");
        client.Create(new AppendBlobCreateOptions());

        var props = client.GetProperties().Value;
        Assert.That(props.BlobType, Is.EqualTo(BlobType.Append));
    }

    [Test]
    public void AppendBlock_Without_Create_Throws_404()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "nope");
        container.CreateIfNotExists();

        var client = container.GetFileAppendBlobClient("missing.txt");
        var ex = Assert.Throws<RequestFailedException>(() =>
            client.AppendBlock(new MemoryStream("x"u8.ToArray())));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    [Test]
    public void AppendBlock_Grows_File_Size()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "grow");
        container.CreateIfNotExists();

        var client = container.GetFileAppendBlobClient("growing.bin");
        client.Create(new AppendBlobCreateOptions());

        client.AppendBlock(new MemoryStream(new byte[100]));
        Assert.That(client.GetProperties().Value.ContentLength, Is.EqualTo(100));

        client.AppendBlock(new MemoryStream(new byte[50]));
        Assert.That(client.GetProperties().Value.ContentLength, Is.EqualTo(150));
    }

    [Test]
    public async Task Create_And_AppendBlock_Roundtrip_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "append");
        await container.CreateIfNotExistsAsync();

        var client = container.GetFileAppendBlobClient("log.txt");
        await client.CreateAsync(new AppendBlobCreateOptions());

        await client.AppendBlockAsync(new MemoryStream("line 1\n"u8.ToArray()));
        await client.AppendBlockAsync(new MemoryStream("line 2\n"u8.ToArray()));

        var content = (await client.DownloadContentAsync()).Value.Content.ToString();
        Assert.That(content, Is.EqualTo("line 1\nline 2\n"));
    }

    [Test]
    public async Task Create_Sets_BlobType_Append_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "appendtype");
        await container.CreateIfNotExistsAsync();

        var client = container.GetFileAppendBlobClient("typed.txt");
        await client.CreateAsync(new AppendBlobCreateOptions());

        var props = (await client.GetPropertiesAsync()).Value;
        Assert.That(props.BlobType, Is.EqualTo(BlobType.Append));
    }

    [Test]
    public void AppendBlock_Without_Create_Throws_404_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "nope");
        container.CreateIfNotExists();

        var client = container.GetFileAppendBlobClient("missing.txt");
        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.AppendBlockAsync(new MemoryStream("x"u8.ToArray())));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    [Test]
    public async Task AppendBlock_Grows_File_Size_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "grow");
        await container.CreateIfNotExistsAsync();

        var client = container.GetFileAppendBlobClient("growing.bin");
        await client.CreateAsync(new AppendBlobCreateOptions());

        await client.AppendBlockAsync(new MemoryStream(new byte[100]));
        Assert.That((await client.GetPropertiesAsync()).Value.ContentLength, Is.EqualTo(100));

        await client.AppendBlockAsync(new MemoryStream(new byte[50]));
        Assert.That((await client.GetPropertiesAsync()).Value.ContentLength, Is.EqualTo(150));
    }
}
