using Azure;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobETagTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    [Test]
    public void Upload_With_IfNoneMatch_Star_Prevents_Overwrite()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-test");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("once.txt");
        client.Upload(BinaryData.FromString("first"));

        var ex = Assert.Throws<RequestFailedException>(() =>
            client.Upload(BinaryData.FromString("second"), overwrite: false));
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    [Test]
    public void Upload_Returns_ETag()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-ret");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("etag.txt");
        var result = client.Upload(BinaryData.FromString("test"));

        Assert.That(result.Value.ETag.ToString(), Does.StartWith("\"0x"));
    }

    [Test]
    public void Delete_With_Stale_IfMatch_Throws_412()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-del");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("versioned.txt");
        client.Upload(BinaryData.FromString("v1"));
        var etag1 = client.GetProperties().Value.ETag;

        // Modify to change ETag.
        client.Upload(BinaryData.FromString("v2"), overwrite: true);

        var ex = Assert.Throws<RequestFailedException>(() =>
            client.Delete(conditions: new BlobRequestConditions { IfMatch = etag1 }));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    [Test]
    public void ETag_Changes_On_Every_Write()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-change");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("changing.txt");
        client.Upload(BinaryData.FromString("v1"));
        var etag1 = client.GetProperties().Value.ETag;

        client.Upload(BinaryData.FromString("v2"), overwrite: true);
        var etag2 = client.GetProperties().Value.ETag;

        client.Upload(BinaryData.FromString("v3"), overwrite: true);
        var etag3 = client.GetProperties().Value.ETag;

        Assert.That(etag1, Is.Not.EqualTo(etag2));
        Assert.That(etag2, Is.Not.EqualTo(etag3));
        Assert.That(etag1, Is.Not.EqualTo(etag3));
    }

    [Test]
    public void Upload_With_Correct_IfMatch_Succeeds()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-ok");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("guarded.txt");
        client.Upload(BinaryData.FromString("v1"));
        var currentETag = client.GetProperties().Value.ETag;

        Assert.DoesNotThrow(() =>
            client.Upload(BinaryData.FromString("v2"), new BlobUploadOptions
            {
                Conditions = new BlobRequestConditions { IfMatch = currentETag }
            }));

        Assert.That(client.DownloadContent().Value.Content.ToString(), Is.EqualTo("v2"));
    }

    [Test]
    public void Upload_With_Stale_IfMatch_Throws_412()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-stale");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("race.txt");
        client.Upload(BinaryData.FromString("v1"));
        var staleETag = client.GetProperties().Value.ETag;

        // Simulate another writer updating the blob.
        client.Upload(BinaryData.FromString("v2"), overwrite: true);

        var ex = Assert.Throws<RequestFailedException>(() =>
            client.Upload(BinaryData.FromString("v3"), new BlobUploadOptions
            {
                Conditions = new BlobRequestConditions { IfMatch = staleETag }
            }));
        Assert.That(ex!.Status, Is.EqualTo(412));

        // Original v2 content is preserved.
        Assert.That(client.DownloadContent().Value.Content.ToString(), Is.EqualTo("v2"));
    }

    [Test]
    public void Delete_With_Correct_IfMatch_Succeeds()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-del-ok");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("delete-me.txt");
        client.Upload(BinaryData.FromString("data"));
        var currentETag = client.GetProperties().Value.ETag;

        Assert.DoesNotThrow(() =>
            client.Delete(conditions: new BlobRequestConditions { IfMatch = currentETag }));
        Assert.That(client.Exists().Value, Is.False);
    }

    [Test]
    public void SetMetadata_With_Stale_IfMatch_Throws_412()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-meta");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("meta-guard.txt");
        client.Upload(BinaryData.FromString("data"));
        var staleETag = client.GetProperties().Value.ETag;

        // Mutate the blob so the ETag advances.
        client.SetMetadata(new Dictionary<string, string> { ["k"] = "v1" });

        var ex = Assert.Throws<RequestFailedException>(() =>
            client.SetMetadata(new Dictionary<string, string> { ["k"] = "v2" },
                new BlobRequestConditions { IfMatch = staleETag }));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    [Test]
    public void Optimistic_Concurrency_ReadModifyWrite_Pattern()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "optimistic");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("counter.txt");
        client.Upload(BinaryData.FromString("0"));

        // Simulate the optimistic concurrency loop: read, modify, write-with-ETag.
        var props = client.GetProperties().Value;
        var currentValue = int.Parse(client.DownloadContent().Value.Content.ToString());
        var newValue = currentValue + 1;

        client.Upload(BinaryData.FromString(newValue.ToString()), new BlobUploadOptions
        {
            Conditions = new BlobRequestConditions { IfMatch = props.ETag }
        });

        Assert.That(client.DownloadContent().Value.Content.ToString(), Is.EqualTo("1"));

        // A second attempt with the old ETag should fail.
        var ex = Assert.Throws<RequestFailedException>(() =>
            client.Upload(BinaryData.FromString("999"), new BlobUploadOptions
            {
                Conditions = new BlobRequestConditions { IfMatch = props.ETag }
            }));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    [Test]
    public void IfNoneMatch_Star_Allows_First_Upload()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-create");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("new-only.txt");

        // First upload with IfNoneMatch=* should succeed (blob doesn't exist).
        Assert.DoesNotThrow(() =>
            client.Upload(BinaryData.FromString("created"), overwrite: false));

        Assert.That(client.DownloadContent().Value.Content.ToString(), Is.EqualTo("created"));
    }

    // ───────────────────── Async counterparts ─────────────────────

    [Test]
    public async Task Upload_With_IfNoneMatch_Star_Prevents_Overwrite_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-test");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("once.txt");
        await client.UploadAsync(BinaryData.FromString("first"));

        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.UploadAsync(BinaryData.FromString("second"), overwrite: false));
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    [Test]
    public async Task Upload_Returns_ETag_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-ret");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("etag.txt");
        var result = await client.UploadAsync(BinaryData.FromString("test"));

        Assert.That(result.Value.ETag.ToString(), Does.StartWith("\"0x"));
    }

    [Test]
    public async Task Delete_With_Stale_IfMatch_Throws_412_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-del");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("versioned.txt");
        await client.UploadAsync(BinaryData.FromString("v1"));
        var etag1 = (await client.GetPropertiesAsync()).Value.ETag;

        // Modify to change ETag.
        await client.UploadAsync(BinaryData.FromString("v2"), overwrite: true);

        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.DeleteAsync(conditions: new BlobRequestConditions { IfMatch = etag1 }));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    [Test]
    public async Task ETag_Changes_On_Every_Write_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-change");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("changing.txt");
        await client.UploadAsync(BinaryData.FromString("v1"));
        var etag1 = (await client.GetPropertiesAsync()).Value.ETag;

        await client.UploadAsync(BinaryData.FromString("v2"), overwrite: true);
        var etag2 = (await client.GetPropertiesAsync()).Value.ETag;

        await client.UploadAsync(BinaryData.FromString("v3"), overwrite: true);
        var etag3 = (await client.GetPropertiesAsync()).Value.ETag;

        Assert.That(etag1, Is.Not.EqualTo(etag2));
        Assert.That(etag2, Is.Not.EqualTo(etag3));
        Assert.That(etag1, Is.Not.EqualTo(etag3));
    }

    [Test]
    public async Task Upload_With_Correct_IfMatch_Succeeds_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-ok");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("guarded.txt");
        await client.UploadAsync(BinaryData.FromString("v1"));
        var currentETag = (await client.GetPropertiesAsync()).Value.ETag;

        Assert.DoesNotThrowAsync(async () =>
            await client.UploadAsync(BinaryData.FromString("v2"), new BlobUploadOptions
            {
                Conditions = new BlobRequestConditions { IfMatch = currentETag }
            }));

        Assert.That((await client.DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo("v2"));
    }

    [Test]
    public async Task Upload_With_Stale_IfMatch_Throws_412_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-stale");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("race.txt");
        await client.UploadAsync(BinaryData.FromString("v1"));
        var staleETag = (await client.GetPropertiesAsync()).Value.ETag;

        // Simulate another writer updating the blob.
        await client.UploadAsync(BinaryData.FromString("v2"), overwrite: true);

        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.UploadAsync(BinaryData.FromString("v3"), new BlobUploadOptions
            {
                Conditions = new BlobRequestConditions { IfMatch = staleETag }
            }));
        Assert.That(ex!.Status, Is.EqualTo(412));

        // Original v2 content is preserved.
        Assert.That((await client.DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo("v2"));
    }

    [Test]
    public async Task Delete_With_Correct_IfMatch_Succeeds_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-del-ok");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("delete-me.txt");
        await client.UploadAsync(BinaryData.FromString("data"));
        var currentETag = (await client.GetPropertiesAsync()).Value.ETag;

        Assert.DoesNotThrowAsync(async () =>
            await client.DeleteAsync(conditions: new BlobRequestConditions { IfMatch = currentETag }));
        Assert.That((await client.ExistsAsync()).Value, Is.False);
    }

    [Test]
    public async Task SetMetadata_With_Stale_IfMatch_Throws_412_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-meta");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("meta-guard.txt");
        await client.UploadAsync(BinaryData.FromString("data"));
        var staleETag = (await client.GetPropertiesAsync()).Value.ETag;

        // Mutate the blob so the ETag advances.
        await client.SetMetadataAsync(new Dictionary<string, string> { ["k"] = "v1" });

        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.SetMetadataAsync(new Dictionary<string, string> { ["k"] = "v2" },
                new BlobRequestConditions { IfMatch = staleETag }));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    [Test]
    public async Task Optimistic_Concurrency_ReadModifyWrite_Pattern_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "optimistic");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("counter.txt");
        await client.UploadAsync(BinaryData.FromString("0"));

        // Simulate the optimistic concurrency loop: read, modify, write-with-ETag.
        var props = (await client.GetPropertiesAsync()).Value;
        var currentValue = int.Parse((await client.DownloadContentAsync()).Value.Content.ToString());
        var newValue = currentValue + 1;

        await client.UploadAsync(BinaryData.FromString(newValue.ToString()), new BlobUploadOptions
        {
            Conditions = new BlobRequestConditions { IfMatch = props.ETag }
        });

        Assert.That((await client.DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo("1"));

        // A second attempt with the old ETag should fail.
        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.UploadAsync(BinaryData.FromString("999"), new BlobUploadOptions
            {
                Conditions = new BlobRequestConditions { IfMatch = props.ETag }
            }));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    [Test]
    public async Task IfNoneMatch_Star_Allows_First_Upload_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "etag-create");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("new-only.txt");

        // First upload with IfNoneMatch=* should succeed (blob doesn't exist).
        Assert.DoesNotThrowAsync(async () =>
            await client.UploadAsync(BinaryData.FromString("created"), overwrite: false));

        Assert.That((await client.DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo("created"));
    }
}
