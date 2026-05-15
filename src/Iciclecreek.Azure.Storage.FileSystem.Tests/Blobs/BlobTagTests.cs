using Azure;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobTagTests
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
    public void SetTags_And_GetTags_Roundtrip()
    {
        var bc = CreateBlob("tags-test", "myblob.txt");
        var tags = new Dictionary<string, string> { ["env"] = "prod", ["team"] = "backend" };
        bc.SetTags(tags);

        var result = bc.GetTags().Value;
        Assert.That(result.Tags["env"], Is.EqualTo("prod"));
        Assert.That(result.Tags["team"], Is.EqualTo("backend"));
    }

    [Test]
    public async Task SetTagsAsync_And_GetTagsAsync_Roundtrip()
    {
        var bc = CreateBlob("tags-async", "myblob.txt");
        await bc.SetTagsAsync(new Dictionary<string, string> { ["version"] = "42" });

        var result = (await bc.GetTagsAsync()).Value;
        Assert.That(result.Tags["version"], Is.EqualTo("42"));
    }

    [Test]
    public void SetTags_Overwrites_Previous_Tags()
    {
        var bc = CreateBlob("tags-overwrite", "myblob.txt");
        bc.SetTags(new Dictionary<string, string> { ["old"] = "value" });
        bc.SetTags(new Dictionary<string, string> { ["new"] = "value2" });

        var result = bc.GetTags().Value;
        Assert.That(result.Tags.ContainsKey("old"), Is.False);
        Assert.That(result.Tags["new"], Is.EqualTo("value2"));
    }

    [Test]
    public void GetTags_Returns_Empty_When_No_Tags()
    {
        var bc = CreateBlob("tags-empty", "myblob.txt");
        var result = bc.GetTags().Value;
        Assert.That(result.Tags, Is.Empty);
    }

    [Test]
    public void GetTags_Throws_404_For_Missing_Blob()
    {
        var cc = FileBlobContainerClient.FromAccount(_root.Account, "tags-missing");
        cc.CreateIfNotExists();
        var bc = (FileBlobClient)cc.GetBlobClient("nope.txt");

        var ex = Assert.Throws<RequestFailedException>(() => bc.GetTags());
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    [Test]
    public void SetTags_Throws_404_For_Missing_Blob()
    {
        var cc = FileBlobContainerClient.FromAccount(_root.Account, "tags-missing2");
        cc.CreateIfNotExists();
        var bc = (FileBlobClient)cc.GetBlobClient("nope.txt");

        var ex = Assert.Throws<RequestFailedException>(() => bc.SetTags(new Dictionary<string, string> { ["x"] = "y" }));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }
}
