using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Blobs;

[TestFixture]
public abstract class BlobClientRoundtripTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void SetUp() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── Upload_Download_Roundtrip_Text ─────────────────────────────────

    [Test]
    public void Upload_Download_Roundtrip_Text()
    {
        var container = _fixture.CreateBlobContainerClient("test-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("hello.txt");
        client.Upload(BinaryData.FromString("Hello, World!"));

        var downloaded = client.DownloadContent();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("Hello, World!"));
    }

    [Test]
    public async Task Upload_Download_Roundtrip_Text_Async()
    {
        var container = _fixture.CreateBlobContainerClient("test-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("hello.txt");
        await client.UploadAsync(BinaryData.FromString("Hello, World!"));

        var downloaded = await client.DownloadContentAsync();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("Hello, World!"));
    }

    // ── Upload_Download_Roundtrip_Binary ───────────────────────────────

    [Test]
    public void Upload_Download_Roundtrip_Binary()
    {
        var container = _fixture.CreateBlobContainerClient("test-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("data.bin");
        var original = new byte[] { 0x00, 0xFF, 0xAA, 0x55 };
        client.Upload(BinaryData.FromBytes(original));

        var downloaded = client.DownloadContent();
        Assert.That(downloaded.Value.Content.ToArray(), Is.EqualTo(original));
    }

    [Test]
    public async Task Upload_Download_Roundtrip_Binary_Async()
    {
        var container = _fixture.CreateBlobContainerClient("test-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("data.bin");
        var original = new byte[] { 0x00, 0xFF, 0xAA, 0x55 };
        await client.UploadAsync(BinaryData.FromBytes(original));

        var downloaded = await client.DownloadContentAsync();
        Assert.That(downloaded.Value.Content.ToArray(), Is.EqualTo(original));
    }

    // ── Exists_Returns_False_For_Missing_Blob ──────────────────────────

    [Test]
    public void Exists_Returns_False_For_Missing_Blob()
    {
        var container = _fixture.CreateBlobContainerClient("test-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("missing.txt");
        Assert.That(client.Exists().Value, Is.False);
    }

    [Test]
    public async Task Exists_Returns_False_For_Missing_Blob_Async()
    {
        var container = _fixture.CreateBlobContainerClient("test-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("missing.txt");
        Assert.That((await client.ExistsAsync()).Value, Is.False);
    }

    // ── Exists_Returns_True_After_Upload ───────────────────────────────

    [Test]
    public void Exists_Returns_True_After_Upload()
    {
        var container = _fixture.CreateBlobContainerClient("test-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("exists.txt");
        client.Upload(BinaryData.FromString("data"));
        Assert.That(client.Exists().Value, Is.True);
    }

    [Test]
    public async Task Exists_Returns_True_After_Upload_Async()
    {
        var container = _fixture.CreateBlobContainerClient("test-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("exists.txt");
        await client.UploadAsync(BinaryData.FromString("data"));
        Assert.That((await client.ExistsAsync()).Value, Is.True);
    }

    // ── Delete_Removes_Blob ────────────────────────────────────────────

    [Test]
    public void Delete_Removes_Blob()
    {
        var container = _fixture.CreateBlobContainerClient("test-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("to-delete.txt");
        client.Upload(BinaryData.FromString("data"));
        Assert.That(client.Exists().Value, Is.True);

        client.Delete();
        Assert.That(client.Exists().Value, Is.False);
    }

    [Test]
    public async Task Delete_Removes_Blob_Async()
    {
        var container = _fixture.CreateBlobContainerClient("test-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("to-delete.txt");
        await client.UploadAsync(BinaryData.FromString("data"));
        Assert.That((await client.ExistsAsync()).Value, Is.True);

        await client.DeleteAsync();
        Assert.That((await client.ExistsAsync()).Value, Is.False);
    }

    // ── GetProperties_Returns_ContentType_And_Length ────────────────────

    [Test]
    public void GetProperties_Returns_ContentType_And_Length()
    {
        var container = _fixture.CreateBlobContainerClient("test-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("props.txt");
        var content = "Hello, World!";
        client.Upload(BinaryData.FromString(content), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = "text/plain" }
        });

        var props = client.GetProperties().Value;
        Assert.That(props.ContentLength, Is.EqualTo(content.Length));
        Assert.That(props.ContentType, Is.EqualTo("text/plain"));
        Assert.That(props.ETag, Is.Not.EqualTo(default(ETag)));
    }

    [Test]
    public async Task GetProperties_Returns_ContentType_And_Length_Async()
    {
        var container = _fixture.CreateBlobContainerClient("test-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("props.txt");
        var content = "Hello, World!";
        await client.UploadAsync(BinaryData.FromString(content), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = "text/plain" }
        });

        var props = (await client.GetPropertiesAsync()).Value;
        Assert.That(props.ContentLength, Is.EqualTo(content.Length));
        Assert.That(props.ContentType, Is.EqualTo("text/plain"));
        Assert.That(props.ETag, Is.Not.EqualTo(default(ETag)));
    }

    // ── SetMetadata_Persists ───────────────────────────────────────────

    [Test]
    public void SetMetadata_Persists()
    {
        var container = _fixture.CreateBlobContainerClient("test-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("meta.txt");
        client.Upload(BinaryData.FromString("data"));

        var metadata = new Dictionary<string, string>
        {
            ["author"] = "test",
            ["version"] = "1"
        };
        client.SetMetadata(metadata);

        var props = client.GetProperties().Value;
        Assert.That(props.Metadata["author"], Is.EqualTo("test"));
        Assert.That(props.Metadata["version"], Is.EqualTo("1"));
    }

    [Test]
    public async Task SetMetadata_Persists_Async()
    {
        var container = _fixture.CreateBlobContainerClient("test-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("meta.txt");
        await client.UploadAsync(BinaryData.FromString("data"));

        var metadata = new Dictionary<string, string>
        {
            ["author"] = "test",
            ["version"] = "1"
        };
        await client.SetMetadataAsync(metadata);

        var props = (await client.GetPropertiesAsync()).Value;
        Assert.That(props.Metadata["author"], Is.EqualTo("test"));
        Assert.That(props.Metadata["version"], Is.EqualTo("1"));
    }
}
