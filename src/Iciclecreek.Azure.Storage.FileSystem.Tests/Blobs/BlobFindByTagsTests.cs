using Azure;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobFindByTagsTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    private FileBlobClient CreateBlobWithTags(string container, string blob, Dictionary<string, string> tags)
    {
        var cc = FileBlobContainerClient.FromAccount(_root.Account, container);
        cc.CreateIfNotExists();
        var bc = (FileBlobClient)cc.GetBlobClient(blob);
        bc.Upload(BinaryData.FromString("data"));
        bc.SetTags(tags);
        return bc;
    }

    [Test]
    public void FindBlobsByTags_FindsTaggedBlobs()
    {
        CreateBlobWithTags("find-tags", "a.txt", new Dictionary<string, string> { ["env"] = "prod" });
        CreateBlobWithTags("find-tags", "b.txt", new Dictionary<string, string> { ["env"] = "dev" });

        var service = FileBlobServiceClient.FromAccount(_root.Account);
        var results = service.FindBlobsByTags("env = 'prod'").ToList();

        Assert.That(results.Count, Is.EqualTo(1));
        Assert.That(results[0].BlobName, Is.EqualTo("a.txt"));
    }

    [Test]
    public void FindBlobsByTags_ReturnsEmpty_WhenNoMatch()
    {
        CreateBlobWithTags("find-empty", "a.txt", new Dictionary<string, string> { ["env"] = "prod" });

        var service = FileBlobServiceClient.FromAccount(_root.Account);
        var results = service.FindBlobsByTags("env = 'staging'").ToList();

        Assert.That(results, Is.Empty);
    }

    [Test]
    public void FindBlobsByTags_WithMultipleConditions()
    {
        CreateBlobWithTags("find-multi", "match.txt", new Dictionary<string, string>
        {
            ["env"] = "prod",
            ["team"] = "backend"
        });
        CreateBlobWithTags("find-multi", "partial.txt", new Dictionary<string, string>
        {
            ["env"] = "prod",
            ["team"] = "frontend"
        });

        var service = FileBlobServiceClient.FromAccount(_root.Account);
        var results = service.FindBlobsByTags("env = 'prod' AND team = 'backend'").ToList();

        Assert.That(results.Count, Is.EqualTo(1));
        Assert.That(results[0].BlobName, Is.EqualTo("match.txt"));
    }

    [Test]
    public void FindBlobsByTags_AcrossContainers()
    {
        CreateBlobWithTags("container-a", "one.txt", new Dictionary<string, string> { ["project"] = "alpha" });
        CreateBlobWithTags("container-b", "two.txt", new Dictionary<string, string> { ["project"] = "alpha" });
        CreateBlobWithTags("container-c", "three.txt", new Dictionary<string, string> { ["project"] = "beta" });

        var service = FileBlobServiceClient.FromAccount(_root.Account);
        var results = service.FindBlobsByTags("project = 'alpha'").ToList();

        Assert.That(results.Count, Is.EqualTo(2));
        var blobNames = results.Select(r => r.BlobName).OrderBy(n => n).ToArray();
        Assert.That(blobNames, Is.EqualTo(new[] { "one.txt", "two.txt" }));
    }
}
