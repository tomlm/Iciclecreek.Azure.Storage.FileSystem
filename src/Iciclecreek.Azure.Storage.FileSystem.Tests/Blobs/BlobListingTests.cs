using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobListingTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    [Test]
    public void GetBlobs_Lists_Uploaded_Blobs()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "listing");
        container.CreateIfNotExists();

        container.UploadBlob("a.txt", BinaryData.FromString("a"));
        container.UploadBlob("b.txt", BinaryData.FromString("b"));

        var names = container.GetBlobs(BlobTraits.None, BlobStates.None).Select(b => b.Name).OrderBy(n => n).ToArray();
        Assert.That(names, Is.EqualTo(new[] { "a.txt", "b.txt" }));
    }

    [Test]
    public void GetBlobs_Does_Not_Include_Sidecars()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "sidecars");
        container.CreateIfNotExists();

        container.UploadBlob("data.txt", BinaryData.FromString("stuff"));

        var names = container.GetBlobs(BlobTraits.None, BlobStates.None).Select(b => b.Name).ToArray();
        Assert.That(names, Does.Not.Contain("data.txt.meta.json"));
        Assert.That(names, Has.Length.EqualTo(1));
    }

    [Test]
    public void GetBlobs_With_Prefix_Filters()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "prefix");
        container.CreateIfNotExists();

        container.UploadBlob("images/cat.jpg", BinaryData.FromString("cat"));
        container.UploadBlob("images/dog.jpg", BinaryData.FromString("dog"));
        container.UploadBlob("docs/readme.md", BinaryData.FromString("readme"));

        var names = container.GetBlobs(prefix: "images/").Select(b => b.Name).OrderBy(n => n).ToArray();
        Assert.That(names, Is.EqualTo(new[] { "images/cat.jpg", "images/dog.jpg" }));
    }

    [Test]
    public void GetBlobsByHierarchy_Returns_Prefixes_And_Blobs()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "hierarchy");
        container.CreateIfNotExists();

        container.UploadBlob("images/cat.jpg", BinaryData.FromString("cat"));
        container.UploadBlob("images/dog.jpg", BinaryData.FromString("dog"));
        container.UploadBlob("readme.md", BinaryData.FromString("readme"));

        var items = container.GetBlobsByHierarchy(delimiter: "/").ToArray();

        var prefixes = items.Where(i => i.IsPrefix).Select(i => i.Prefix).OrderBy(p => p).ToArray();
        var blobs = items.Where(i => i.IsBlob).Select(i => i.Blob.Name).ToArray();

        Assert.That(prefixes, Is.EqualTo(new[] { "images/" }));
        Assert.That(blobs, Is.EqualTo(new[] { "readme.md" }));
    }

    [Test]
    public void GetBlobs_With_Metadata_Trait()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "metatrait");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("tagged.txt");
        client.Upload(BinaryData.FromString("tagged"));
        client.SetMetadata(new Dictionary<string, string> { ["env"] = "test" });

        var blob = container.GetBlobs(BlobTraits.Metadata).First();
        Assert.That(blob.Metadata["env"], Is.EqualTo("test"));
    }
}
