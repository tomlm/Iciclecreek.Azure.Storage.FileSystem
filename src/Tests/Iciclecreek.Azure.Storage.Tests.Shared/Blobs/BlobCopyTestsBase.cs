using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Blobs;

[TestFixture]
public abstract class BlobCopyTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void SetUp() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    protected BlobClient CreateBlob(string container, string blob, string content = "test")
    {
        var cc = _fixture.CreateBlobContainerClient(container);
        cc.CreateIfNotExists();
        var bc = cc.GetBlobClient(blob);
        bc.Upload(BinaryData.FromString(content));
        return bc;
    }

    // ── StartCopyFromUri_Copies_Local_Blob ─────────────────────────────

    [Test]
    public void StartCopyFromUri_Copies_Local_Blob()
    {
        var source = CreateBlob("copy-src", "source.txt", "copy me");

        var destContainer = _fixture.CreateBlobContainerClient("copy-dst");
        destContainer.CreateIfNotExists();
        var dest = destContainer.GetBlobClient("dest.txt");

        dest.StartCopyFromUri(source.Uri, (BlobCopyFromUriOptions?)null);

        var downloaded = dest.DownloadContent();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("copy me"));
    }

    // ── StartCopyFromUriAsync_Copies_Local_Blob ────────────────────────

    [Test]
    public async Task StartCopyFromUriAsync_Copies_Local_Blob()
    {
        var source = CreateBlob("copy-src-async", "source.txt", "copy me async");

        var destContainer = _fixture.CreateBlobContainerClient("copy-dst-async");
        await destContainer.CreateIfNotExistsAsync();
        var dest = destContainer.GetBlobClient("dest.txt");

        await dest.StartCopyFromUriAsync(source.Uri, (BlobCopyFromUriOptions?)null);

        var downloaded = await dest.DownloadContentAsync();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("copy me async"));
    }

    // ── SyncCopyFromUri_Copies_And_Returns_Success ─────────────────────

    [Test]
    public void SyncCopyFromUri_Copies_And_Returns_Success()
    {
        var source = CreateBlob("sync-copy-src", "source.txt", "sync copy");

        var destContainer = _fixture.CreateBlobContainerClient("sync-copy-dst");
        destContainer.CreateIfNotExists();
        var dest = destContainer.GetBlobClient("dest.txt");

        var result = dest.SyncCopyFromUri(source.Uri);
        Assert.That(result.Value.CopyStatus, Is.EqualTo(CopyStatus.Success));
        Assert.That(result.Value.CopyId, Is.Not.Null.And.Not.Empty);
    }

    // ── SyncCopyFromUriAsync_Copies_And_Returns_Success ────────────────

    [Test]
    public async Task SyncCopyFromUriAsync_Copies_And_Returns_Success()
    {
        var source = CreateBlob("sync-copy-src-async", "source.txt", "sync copy async");

        var destContainer = _fixture.CreateBlobContainerClient("sync-copy-dst-async");
        await destContainer.CreateIfNotExistsAsync();
        var dest = destContainer.GetBlobClient("dest.txt");

        var result = await dest.SyncCopyFromUriAsync(source.Uri);
        Assert.That(result.Value.CopyStatus, Is.EqualTo(CopyStatus.Success));
        Assert.That(result.Value.CopyId, Is.Not.Null.And.Not.Empty);
    }

    // ── Copy_Between_Containers ────────────────────────────────────────

    [Test]
    public void Copy_Between_Containers()
    {
        var source = CreateBlob("cross-src", "data.txt", "cross container");

        var destContainer = _fixture.CreateBlobContainerClient("cross-dst");
        destContainer.CreateIfNotExists();
        var dest = destContainer.GetBlobClient("data.txt");

        dest.StartCopyFromUri(source.Uri, (BlobCopyFromUriOptions?)null);

        var downloaded = dest.DownloadContent();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("cross container"));
    }

    // ── Copy_Overwrites_Existing_Blob ──────────────────────────────────

    [Test]
    public void Copy_Overwrites_Existing_Blob()
    {
        var source = CreateBlob("overwrite-src", "source.txt", "new content");
        var existing = CreateBlob("overwrite-dst", "dest.txt", "old content");

        existing.StartCopyFromUri(source.Uri, (BlobCopyFromUriOptions?)null);

        var downloaded = existing.DownloadContent();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("new content"));
    }
}
