using Azure.Storage.Blobs;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Blobs;

[TestFixture]
public abstract class BlobFindByTagsTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void SetUp() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── Helper ─────────────────────────────────────────────────────────

    private BlobClient CreateBlobWithTags(string container, string blob, Dictionary<string, string> tags)
    {
        var cc = _fixture.CreateBlobContainerClient(container);
        cc.CreateIfNotExists();
        var bc = cc.GetBlobClient(blob);
        bc.Upload(BinaryData.FromString("data"));
        bc.SetTags(tags);
        return bc;
    }

    // ── FindBlobsByTags_FindsTaggedBlobs ───────────────────────────────

    [Test]
    public void FindBlobsByTags_FindsTaggedBlobs()
    {
        CreateBlobWithTags("tags-find", "prod.txt", new Dictionary<string, string> { ["env"] = "prod" });
        CreateBlobWithTags("tags-find", "dev.txt", new Dictionary<string, string> { ["env"] = "dev" });

        var service = _fixture.CreateBlobServiceClient();
        var results = new List<string>();
        foreach (var item in service.FindBlobsByTags("env = 'prod'"))
            results.Add(item.BlobName);

        Assert.That(results.Count, Is.EqualTo(1));
        Assert.That(results[0], Is.EqualTo("prod.txt"));
    }

    // ── FindBlobsByTags_ReturnsEmpty_WhenNoMatch ──────────────────────

    [Test]
    public void FindBlobsByTags_ReturnsEmpty_WhenNoMatch()
    {
        CreateBlobWithTags("tags-empty", "prod.txt", new Dictionary<string, string> { ["env"] = "prod" });

        var service = _fixture.CreateBlobServiceClient();
        var results = new List<string>();
        foreach (var item in service.FindBlobsByTags("env = 'staging'"))
            results.Add(item.BlobName);

        Assert.That(results, Is.Empty);
    }

    // ── FindBlobsByTags_WithMultipleConditions ─────────────────────────

    [Test]
    public void FindBlobsByTags_WithMultipleConditions()
    {
        CreateBlobWithTags("tags-multi", "a.txt", new Dictionary<string, string> { ["env"] = "prod", ["team"] = "backend" });
        CreateBlobWithTags("tags-multi", "b.txt", new Dictionary<string, string> { ["env"] = "prod", ["team"] = "frontend" });

        var service = _fixture.CreateBlobServiceClient();
        var results = new List<string>();
        foreach (var item in service.FindBlobsByTags("env = 'prod' AND team = 'backend'"))
            results.Add(item.BlobName);

        Assert.That(results.Count, Is.EqualTo(1));
        Assert.That(results[0], Is.EqualTo("a.txt"));
    }

    // ── FindBlobsByTags_AcrossContainers ──────────────────────────────

    [Test]
    public void FindBlobsByTags_AcrossContainers()
    {
        CreateBlobWithTags("tags-cross-1", "one.txt", new Dictionary<string, string> { ["project"] = "alpha" });
        CreateBlobWithTags("tags-cross-2", "two.txt", new Dictionary<string, string> { ["project"] = "alpha" });
        CreateBlobWithTags("tags-cross-3", "three.txt", new Dictionary<string, string> { ["project"] = "beta" });

        var service = _fixture.CreateBlobServiceClient();
        var results = new List<string>();
        foreach (var item in service.FindBlobsByTags("project = 'alpha'"))
            results.Add(item.BlobName);

        results.Sort();
        Assert.That(results.Count, Is.EqualTo(2));
        Assert.That(results[0], Is.EqualTo("one.txt"));
        Assert.That(results[1], Is.EqualTo("two.txt"));
    }
}
