using Azure;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobCopyTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    private FileBlobClient CreateBlob(string container, string blob, string content = "test")
    {
        var cc = FileBlobContainerClient.FromAccount(_root.Account, container);
        cc.CreateIfNotExists();
        var bc = (FileBlobClient)cc.GetBlobClient(blob);
        bc.Upload(BinaryData.FromString(content));
        return bc;
    }

    [Test]
    public void StartCopyFromUri_Copies_Local_Blob()
    {
        var source = CreateBlob("copy-src", "source.txt", "copy me");
        var destContainer = FileBlobContainerClient.FromAccount(_root.Account, "copy-dst");
        destContainer.CreateIfNotExists();
        var dest = (FileBlobClient)destContainer.GetBlobClient("dest.txt");

        dest.StartCopyFromUri(source.Uri, (BlobCopyFromUriOptions?)null);

        var downloaded = dest.DownloadContent().Value.Content.ToString();
        Assert.That(downloaded, Is.EqualTo("copy me"));
    }

    [Test]
    public async Task StartCopyFromUriAsync_Copies_Local_Blob()
    {
        var source = CreateBlob("copy-src-async", "source.txt", "async copy");
        var destContainer = FileBlobContainerClient.FromAccount(_root.Account, "copy-dst-async");
        destContainer.CreateIfNotExists();
        var dest = (FileBlobClient)destContainer.GetBlobClient("dest.txt");

        await dest.StartCopyFromUriAsync(source.Uri, (BlobCopyFromUriOptions?)null);

        var downloaded = (await dest.DownloadContentAsync()).Value.Content.ToString();
        Assert.That(downloaded, Is.EqualTo("async copy"));
    }

    [Test]
    public void SyncCopyFromUri_Copies_And_Returns_Success()
    {
        var source = CreateBlob("synccopy-src", "source.txt", "sync copy");
        var destContainer = FileBlobContainerClient.FromAccount(_root.Account, "synccopy-dst");
        destContainer.CreateIfNotExists();
        var dest = (FileBlobClient)destContainer.GetBlobClient("dest.txt");

        var result = dest.SyncCopyFromUri(source.Uri).Value;
        Assert.That(result.CopyStatus, Is.EqualTo(CopyStatus.Success));
        Assert.That(result.CopyId, Is.Not.Null.And.Not.Empty);

        var downloaded = dest.DownloadContent().Value.Content.ToString();
        Assert.That(downloaded, Is.EqualTo("sync copy"));
    }

    [Test]
    public async Task SyncCopyFromUriAsync_Copies_And_Returns_Success()
    {
        var source = CreateBlob("synccopy-src-async", "source.txt", "async sync copy");
        var destContainer = FileBlobContainerClient.FromAccount(_root.Account, "synccopy-dst-async");
        destContainer.CreateIfNotExists();
        var dest = (FileBlobClient)destContainer.GetBlobClient("dest.txt");

        var result = (await dest.SyncCopyFromUriAsync(source.Uri)).Value;
        Assert.That(result.CopyStatus, Is.EqualTo(CopyStatus.Success));

        var downloaded = (await dest.DownloadContentAsync()).Value.Content.ToString();
        Assert.That(downloaded, Is.EqualTo("async sync copy"));
    }

    [Test]
    public void Copy_Between_Containers()
    {
        var source = CreateBlob("cross-src", "data.bin", "cross-container");
        var destContainer = FileBlobContainerClient.FromAccount(_root.Account, "cross-dst");
        destContainer.CreateIfNotExists();
        var dest = (FileBlobClient)destContainer.GetBlobClient("data-copy.bin");

        dest.StartCopyFromUri(source.Uri, (BlobCopyFromUriOptions?)null);

        Assert.That(dest.Exists().Value, Is.True);
        var downloaded = dest.DownloadContent().Value.Content.ToString();
        Assert.That(downloaded, Is.EqualTo("cross-container"));
    }

    [Test]
    public void Copy_Overwrites_Existing_Blob()
    {
        var source = CreateBlob("overwrite-src", "new.txt", "new content");
        var dest = CreateBlob("overwrite-dst", "old.txt", "old content");

        dest.StartCopyFromUri(source.Uri, (BlobCopyFromUriOptions?)null);

        var downloaded = dest.DownloadContent().Value.Content.ToString();
        Assert.That(downloaded, Is.EqualTo("new content"));
    }
}
