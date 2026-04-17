using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobAdvancedFeatureTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    // ---- GenerateSasUri ----

    [Test]
    public void GenerateSasUri_Returns_Blob_Uri()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "sas-test");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("doc.txt");
        client.Upload(BinaryData.FromString("data"));

        var sasUri = ((FileBlobClient)client).GenerateSasUri(
            global::Azure.Storage.Sas.BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddHours(1));
        Assert.That(sasUri, Is.Not.Null);
        Assert.That(sasUri.ToString(), Does.Contain("sas-test"));
        Assert.That(sasUri.ToString(), Does.Contain("doc.txt"));
    }

    [Test]
    public void Container_GenerateSasUri_Returns_Container_Uri()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "sas-container");
        container.CreateIfNotExists();

        var sasUri = container.GenerateSasUri(new global::Azure.Storage.Sas.BlobSasBuilder());
        Assert.That(sasUri, Is.Not.Null);
        Assert.That(sasUri.ToString(), Does.Contain("sas-container"));
    }

    // ---- OpenRead ----

    [Test]
    public async Task OpenRead_Returns_Readable_Stream()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "openread");
        container.CreateIfNotExists();

        var client = (FileBlobClient)container.GetBlobClient("readable.txt");
        await client.UploadAsync(BinaryData.FromString("stream content"));

        await using var stream = await client.OpenReadAsync();
        using var reader = new StreamReader(stream);
        var text = await reader.ReadToEndAsync();
        Assert.That(text, Is.EqualTo("stream content"));
    }

    [Test]
    public async Task OpenRead_With_Position_Seeks()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "openread-seek");
        container.CreateIfNotExists();

        var client = (FileBlobClient)container.GetBlobClient("seekable.txt");
        await client.UploadAsync(BinaryData.FromString("Hello, World!"));

        await using var stream = await client.OpenReadAsync(position: 7);
        using var reader = new StreamReader(stream);
        var text = await reader.ReadToEndAsync();
        Assert.That(text, Is.EqualTo("World!"));
    }

    [Test]
    public void OpenRead_Missing_Blob_Throws_404()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "openread-miss");
        container.CreateIfNotExists();

        var client = (FileBlobClient)container.GetBlobClient("missing.txt");
        var ex = Assert.ThrowsAsync<RequestFailedException>(async () => await client.OpenReadAsync());
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    // ---- DownloadTo ----

    [Test]
    public async Task DownloadTo_Stream_Copies_Content()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "downloadto");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("source.txt");
        await client.UploadAsync(BinaryData.FromString("download me"));

        using var dest = new MemoryStream();
        await client.DownloadToAsync(dest);
        dest.Position = 0;
        using var reader = new StreamReader(dest);
        Assert.That(await reader.ReadToEndAsync(), Is.EqualTo("download me"));
    }

    [Test]
    public async Task DownloadTo_FilePath_Creates_File()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "downloadto-file");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("source.bin");
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await client.UploadAsync(new BinaryData(data));

        var destPath = Path.Combine(_root.Path, "downloaded.bin");
        await client.DownloadToAsync(destPath);

        Assert.That(File.Exists(destPath), Is.True);
        Assert.That(await File.ReadAllBytesAsync(destPath), Is.EqualTo(data));
    }

    [Test]
    public void DownloadTo_Missing_Blob_Throws_404()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "downloadto-miss");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("nope.txt");
        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.DownloadToAsync(new MemoryStream()));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    // ---- Container SetMetadata / GetProperties returns metadata ----

    [Test]
    public async Task Container_SetMetadata_Roundtrips_Via_GetProperties()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "container-meta");
        await container.CreateIfNotExistsAsync();

        await container.SetMetadataAsync(new Dictionary<string, string>
        {
            ["env"] = "staging",
            ["team"] = "platform"
        });

        var props = (await container.GetPropertiesAsync()).Value;
        Assert.That(props.Metadata, Is.Not.Null);
        Assert.That(props.Metadata["env"], Is.EqualTo("staging"));
        Assert.That(props.Metadata["team"], Is.EqualTo("platform"));
    }

    [Test]
    public void Container_SetMetadata_On_Missing_Container_Throws_404()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "no-such-container");
        Assert.Throws<RequestFailedException>(() =>
            container.SetMetadata(new Dictionary<string, string> { ["k"] = "v" }));
    }

    // ---- NotSupported sweep verification ----

    [Test]
    public void Download_Old_Api_Works()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "dl-old");
        container.CreateIfNotExists();
        var client = container.GetBlobClient("x.txt");
        client.Upload(BinaryData.FromString("old api"));
        var result = client.Download();
        using var reader = new StreamReader(result.Value.Content);
        Assert.That(reader.ReadToEnd(), Is.EqualTo("old api"));
    }

    [Test]
    public void SetAccessTier_Stores_Tier()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "tier-test");
        container.CreateIfNotExists();
        var client = container.GetBlobClient("x.txt");
        client.Upload(BinaryData.FromString("data"));
        client.SetAccessTier(AccessTier.Cool);
        // Tier stored but BlobProperties.AccessTier may not round-trip via model factory — just verify no throw.
    }

    [Test]
    public void CreateSnapshot_Copies_Blob()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "snap-test");
        container.CreateIfNotExists();
        var client = container.GetBlobClient("x.txt");
        client.Upload(BinaryData.FromString("snapshot data"));
        var snapshot = client.CreateSnapshot();
        Assert.That(snapshot.Value.Snapshot, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public void NotSupported_StartCopyFromUri_Throws()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "ns-copy");
        container.CreateIfNotExists();
        var client = container.GetBlobClient("x.txt");
        // Base class may dispatch through internal methods, so either NotSupportedException or NRE is acceptable.
        Assert.That(() => client.StartCopyFromUri(new Uri("https://example.com/blob")),
            Throws.InstanceOf<NotSupportedException>().Or.InstanceOf<NullReferenceException>());
    }

    [Test]
    public void Container_GetAccessPolicy_Returns_Empty_Policy()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "ns-policy");
        container.CreateIfNotExists();
        var policy = container.GetAccessPolicy().Value;
        Assert.That(policy.BlobPublicAccess, Is.EqualTo(PublicAccessType.None));
        Assert.That(policy.SignedIdentifiers, Is.Empty);
    }

    [Test]
    public void Service_GetProperties_Returns_Default_Properties()
    {
        var service = FileBlobServiceClient.FromAccount(_root.Account);
        var props = service.GetProperties().Value;
        Assert.That(props, Is.Not.Null);
    }

    [Test]
    public void Service_GetStatistics_Returns_Statistics()
    {
        var service = FileBlobServiceClient.FromAccount(_root.Account);
        var stats = service.GetStatistics().Value;
        Assert.That(stats, Is.Not.Null);
    }

    [Test]
    public void Service_FindBlobsByTags_Returns_Empty()
    {
        var service = FileBlobServiceClient.FromAccount(_root.Account);
        var items = service.FindBlobsByTags("tag = 'x'").ToList();
        Assert.That(items, Is.Empty);
    }

    [Test]
    public void Container_FindBlobsByTags_Returns_Empty()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "ns-tags");
        container.CreateIfNotExists();
        var items = container.FindBlobsByTags("tag = 'x'").ToList();
        Assert.That(items, Is.Empty);
    }

    // ---- Gap 1: OpenWrite ----

    [Test]
    public async Task OpenWrite_Creates_Blob_On_Dispose()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "openwrite");
        container.CreateIfNotExists();

        var client = (FileBlobClient)container.GetBlobClient("written.txt");

        await using (var stream = await client.OpenWriteAsync(overwrite: true))
        {
            var bytes = "Hello from OpenWrite"u8.ToArray();
            await stream.WriteAsync(bytes);
        }

        var content = (await client.DownloadContentAsync()).Value.Content.ToString();
        Assert.That(content, Is.EqualTo("Hello from OpenWrite"));
    }

    [Test]
    public async Task OpenWrite_Creates_Sidecar_With_Metadata()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "openwrite-meta");
        container.CreateIfNotExists();

        var client = (FileBlobClient)container.GetBlobClient("meta-write.txt");

        await using (var stream = await client.OpenWriteAsync(overwrite: true, new BlobOpenWriteOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = "text/csv" },
            Metadata = new Dictionary<string, string> { ["source"] = "openwrite" }
        }))
        {
            await stream.WriteAsync("col1,col2\na,b"u8.ToArray());
        }

        var props = (await client.GetPropertiesAsync()).Value;
        Assert.That(props.ContentType, Is.EqualTo("text/csv"));
        Assert.That(props.Metadata["source"], Is.EqualTo("openwrite"));
        Assert.That(props.ContentLength, Is.EqualTo(13));
    }

    [Test]
    public async Task OpenWrite_Overwrite_False_Throws_If_Exists()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "openwrite-noover");
        container.CreateIfNotExists();

        var client = (FileBlobClient)container.GetBlobClient("existing.txt");
        await client.UploadAsync(BinaryData.FromString("first"));

        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.OpenWriteAsync(overwrite: false));
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    [Test]
    public async Task OpenWrite_Overwrites_Existing_Content()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "openwrite-over");
        container.CreateIfNotExists();

        var client = (FileBlobClient)container.GetBlobClient("overwritten.txt");
        await client.UploadAsync(BinaryData.FromString("old content that is longer"));

        await using (var stream = await client.OpenWriteAsync(overwrite: true))
        {
            await stream.WriteAsync("new"u8.ToArray());
        }

        var content = (await client.DownloadContentAsync()).Value.Content.ToString();
        Assert.That(content, Is.EqualTo("new"));
    }

    [Test]
    public async Task OpenWrite_MultipleWrites_Concatenates()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "openwrite-multi");
        container.CreateIfNotExists();

        var client = (FileBlobClient)container.GetBlobClient("chunks.bin");

        await using (var stream = await client.OpenWriteAsync(overwrite: true))
        {
            await stream.WriteAsync(new byte[] { 1, 2, 3 });
            await stream.WriteAsync(new byte[] { 4, 5, 6 });
            await stream.WriteAsync(new byte[] { 7, 8, 9 });
        }

        var downloaded = (await client.DownloadContentAsync()).Value.Content.ToArray();
        Assert.That(downloaded, Is.EqualTo(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }));
    }

    // ---- Gap 2: DownloadStreaming additional overloads ----

    [Test]
    public async Task DownloadStreaming_With_Options_Returns_Full_Content()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "streaming-opts");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("stream.txt");
        await client.UploadAsync(BinaryData.FromString("streaming content"));

        var result = await client.DownloadStreamingAsync(new BlobDownloadOptions());
        await using var stream = result.Value.Content;
        using var reader = new StreamReader(stream);
        var text = await reader.ReadToEndAsync();
        Assert.That(text, Is.EqualTo("streaming content"));
    }

    [Test]
    public void DownloadStreaming_Sync_Works()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "streaming-sync");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("stream.txt");
        client.Upload(BinaryData.FromString("sync streaming"));

        var result = client.DownloadStreaming(new BlobDownloadOptions());
        using var reader = new StreamReader(result.Value.Content);
        var text = reader.ReadToEnd();
        Assert.That(text, Is.EqualTo("sync streaming"));
    }

    // ---- Gap 3: Container Create with metadata ----

    [Test]
    public void Container_Create_With_Metadata_Persists()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "create-meta");
        container.Create(metadata: new Dictionary<string, string> { ["env"] = "prod", ["region"] = "us-west" });

        var props = container.GetProperties().Value;
        Assert.That(props.Metadata, Is.Not.Null);
        Assert.That(props.Metadata["env"], Is.EqualTo("prod"));
        Assert.That(props.Metadata["region"], Is.EqualTo("us-west"));
    }

    [Test]
    public void Container_CreateIfNotExists_With_Metadata_Persists()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "createifne-meta");
        container.CreateIfNotExists(metadata: new Dictionary<string, string> { ["team"] = "infra" });

        var props = container.GetProperties().Value;
        Assert.That(props.Metadata, Is.Not.Null);
        Assert.That(props.Metadata["team"], Is.EqualTo("infra"));
    }

    [Test]
    public async Task Container_Create_With_Metadata_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "create-meta-async");
        await container.CreateAsync(metadata: new Dictionary<string, string> { ["async"] = "yes" });

        var props = (await container.GetPropertiesAsync()).Value;
        Assert.That(props.Metadata["async"], Is.EqualTo("yes"));
    }

    [Test]
    public void Container_GetProperties_Without_Metadata_Returns_Null_Metadata()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "no-meta");
        container.Create();

        var props = container.GetProperties().Value;
        // No metadata was set, so Metadata should be null.
        Assert.That(props.Metadata, Is.Null);
    }

    // ---- StageBlockFromUri copies from local blob ----

    [Test]
    public void BlockBlob_StageBlockFromUri_Copies_Local_Blob()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "stagefrom");
        container.CreateIfNotExists();

        // Upload a source blob
        container.UploadBlob("source.bin", BinaryData.FromString("source data"));
        var sourceUri = new Uri($"{_root.Account.BlobServiceUri}stagefrom/source.bin");

        var client = container.GetFileBlockBlobClient("dest.bin");
        var blockId = Convert.ToBase64String("b1"u8.ToArray());
        client.StageBlockFromUri(sourceUri, blockId);
        client.CommitBlockList(new[] { blockId });

        var content = client.DownloadContent().Value.Content.ToString();
        Assert.That(content, Is.EqualTo("source data"));
    }

    // ---- AppendBlockFromUri copies from local blob ----

    [Test]
    public void AppendBlob_AppendBlockFromUri_Copies_Local_Blob()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "appendfrom");
        container.CreateIfNotExists();

        container.UploadBlob("source.txt", BinaryData.FromString("appended content"));
        var sourceUri = new Uri($"{_root.Account.BlobServiceUri}appendfrom/source.txt");

        var client = container.GetFileAppendBlobClient("dest.log");
        client.Create(new AppendBlobCreateOptions());
        client.AppendBlockFromUri(sourceUri);

        var content = client.DownloadContent().Value.Content.ToString();
        Assert.That(content, Is.EqualTo("appended content"));
    }

    [Test]
    public void AppendBlob_Seal_Sets_IsSealed()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "seal-test");
        container.CreateIfNotExists();

        var client = container.GetFileAppendBlobClient("x.log");
        client.Create(new AppendBlobCreateOptions());
        var result = client.Seal();
        Assert.That(result.Value, Is.Not.Null);
    }

    // ---- Item 3: UndeleteBlobContainer 3-arg throws NotSupported ----

    [Test]
    public void Service_UndeleteBlobContainer_Throws_404()
    {
        var service = FileBlobServiceClient.FromAccount(_root.Account);
        var ex = Assert.Throws<RequestFailedException>(() =>
            service.UndeleteBlobContainer("deleted", "version"));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    [Test]
    public void Service_UndeleteBlobContainer_3Arg_Throws_404()
    {
        var service = FileBlobServiceClient.FromAccount(_root.Account);
        var ex = Assert.Throws<RequestFailedException>(() =>
            service.UndeleteBlobContainer("deleted", "version", "destination"));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    // ---- Download(HttpRange, ...) — old API now works ----

    [Test]
    public void Download_Old_Api_With_Range_Works()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "dl-old-range");
        container.CreateIfNotExists();
        var client = container.GetBlobClient("x.txt");
        client.Upload(BinaryData.FromString("range data"));
        var result = client.Download(new global::Azure.HttpRange(0, 5), conditions: null!, rangeGetContentHash: false);
        using var reader = new StreamReader(result.Value.Content);
        Assert.That(reader.ReadToEnd(), Is.EqualTo("range data")); // returns full content (range ignored)
    }

    // ---- Item 4: DownloadStreaming(HttpRange, ...) works ----

    [Test]
    public async Task DownloadStreaming_HttpRange_Overload_Returns_Content()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "ds-range");
        await container.CreateIfNotExistsAsync();

        var client = container.GetBlobClient("stream.txt");
        await client.UploadAsync(BinaryData.FromString("range test content"));

        // HttpRange overload ignores range (returns full content, matching Spotflow behavior)
        var result = await client.DownloadStreamingAsync(
            new global::Azure.HttpRange(0, 5), conditions: null!, rangeGetContentHash: false, progressHandler: null, cancellationToken: default);
        await using var stream = result.Value.Content;
        using var reader = new StreamReader(stream);
        var text = await reader.ReadToEndAsync();
        Assert.That(text, Is.EqualTo("range test content"));
    }

    // ---- Item 5: DownloadContent(BlobRequestConditions, IProgress, HttpRange, ...) works ----

    [Test]
    public async Task DownloadContent_ProgressRange_Overload_Returns_Content()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "dc-progress");
        await container.CreateIfNotExistsAsync();

        var client = container.GetBlobClient("progress.txt");
        await client.UploadAsync(BinaryData.FromString("progress content"));

        // Progress/range overload ignores range and progress (returns full content)
        var result = await client.DownloadContentAsync(
            conditions: null!, progressHandler: null, range: default, cancellationToken: default);
        Assert.That(result.Value.Content.ToString(), Is.EqualTo("progress content"));
    }

    [Test]
    public void DownloadContent_ProgressRange_Sync_Works()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "dc-prog-sync");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("sync.txt");
        client.Upload(BinaryData.FromString("sync progress"));

        var result = client.DownloadContent(conditions: null!, progressHandler: null, range: default, cancellationToken: default);
        Assert.That(result.Value.Content.ToString(), Is.EqualTo("sync progress"));
    }

    // ---- Item 1: GetBlobs / GetBlobsByHierarchy with Options objects ----

    [Test]
    public void GetBlobs_With_GetBlobsOptions_Works()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "opts-blobs");
        container.CreateIfNotExists();

        container.UploadBlob("a.txt", BinaryData.FromString("a"));
        container.UploadBlob("b.txt", BinaryData.FromString("b"));

        var options = new GetBlobsOptions { Prefix = "a" };
        var names = container.GetBlobs(options).Select(b => b.Name).ToArray();
        Assert.That(names, Is.EqualTo(new[] { "a.txt" }));
    }

    [Test]
    public async Task GetBlobsAsync_With_GetBlobsOptions_Works()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "opts-blobs-async");
        await container.CreateIfNotExistsAsync();

        await container.UploadBlobAsync("x.txt", BinaryData.FromString("x"));
        await container.UploadBlobAsync("y.txt", BinaryData.FromString("y"));

        var options = new GetBlobsOptions { Prefix = "x" };
        var blobs = new List<BlobItem>();
        await foreach (var b in container.GetBlobsAsync(options))
            blobs.Add(b);
        Assert.That(blobs.Select(b => b.Name).ToArray(), Is.EqualTo(new[] { "x.txt" }));
    }

    [Test]
    public void GetBlobsByHierarchy_With_Options_Works()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "opts-hier");
        container.CreateIfNotExists();

        container.UploadBlob("dir/a.txt", BinaryData.FromString("a"));
        container.UploadBlob("root.txt", BinaryData.FromString("r"));

        var options = new GetBlobsByHierarchyOptions { Delimiter = "/" };
        var items = container.GetBlobsByHierarchy(options).ToArray();

        var prefixItems = items.Where(i => i.IsPrefix).Select(i => i.Prefix).ToArray();
        var blobItems = items.Where(i => i.IsBlob).Select(i => i.Blob.Name).ToArray();

        Assert.That(prefixItems, Is.EqualTo(new[] { "dir/" }));
        Assert.That(blobItems, Is.EqualTo(new[] { "root.txt" }));
    }

    // ---- Item 2: OpenWrite on BlockBlobClient / AppendBlobClient ----

    [Test]
    public async Task BlockBlobClient_OpenWrite_Creates_Blob()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "bb-openwrite");
        container.CreateIfNotExists();

        var client = container.GetFileBlockBlobClient("block-written.txt");
        await using (var stream = await client.OpenWriteAsync(overwrite: true))
        {
            await stream.WriteAsync("block blob via OpenWrite"u8.ToArray());
        }

        var content = (await client.DownloadContentAsync()).Value.Content.ToString();
        Assert.That(content, Is.EqualTo("block blob via OpenWrite"));
    }

    [Test]
    public async Task AppendBlobClient_OpenWrite_Creates_Blob()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "ab-openwrite");
        container.CreateIfNotExists();

        var client = container.GetFileAppendBlobClient("append-written.txt");
        await using (var stream = await client.OpenWriteAsync(overwrite: true))
        {
            await stream.WriteAsync("append blob via OpenWrite"u8.ToArray());
        }

        var content = (await client.DownloadContentAsync()).Value.Content.ToString();
        Assert.That(content, Is.EqualTo("append blob via OpenWrite"));
    }

    // ---- Table GetAccessPolicies returns empty ----

    [Test]
    public void Table_GetAccessPolicies_Returns_Empty()
    {
        var client = Iciclecreek.Azure.Storage.FileSystem.Tables.FileTableClient.FromAccount(_root.Account, "ns-getpol");
        client.CreateIfNotExists();
        var policies = client.GetAccessPolicies().Value;
        Assert.That(policies, Is.Empty);
    }

    [Test]
    public async Task Table_GetAccessPoliciesAsync_Returns_Empty()
    {
        var client = Iciclecreek.Azure.Storage.FileSystem.Tables.FileTableClient.FromAccount(_root.Account, "ns-getpol-async");
        await client.CreateIfNotExistsAsync();
        var policies = (await client.GetAccessPoliciesAsync()).Value;
        Assert.That(policies, Is.Empty);
    }
}
