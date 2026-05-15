using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Blobs;

[TestFixture]
public abstract class BlobTagTestsBase
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

    // ── SetTags_And_GetTags_Roundtrip ──────────────────────────────────

    [Test]
    public void SetTags_And_GetTags_Roundtrip()
    {
        var client = CreateBlob("tag-container", "tagged.txt");

        var tags = new Dictionary<string, string>
        {
            ["project"] = "alpha",
            ["env"] = "test"
        };
        client.SetTags(tags);

        var result = client.GetTags().Value.Tags;
        Assert.That(result["project"], Is.EqualTo("alpha"));
        Assert.That(result["env"], Is.EqualTo("test"));
    }

    // ── SetTagsAsync_And_GetTagsAsync_Roundtrip ────────────────────────

    [Test]
    public async Task SetTagsAsync_And_GetTagsAsync_Roundtrip()
    {
        var client = CreateBlob("tag-container", "tagged.txt");

        var tags = new Dictionary<string, string>
        {
            ["project"] = "alpha",
            ["env"] = "test"
        };
        await client.SetTagsAsync(tags);

        var result = (await client.GetTagsAsync()).Value.Tags;
        Assert.That(result["project"], Is.EqualTo("alpha"));
        Assert.That(result["env"], Is.EqualTo("test"));
    }

    // ── SetTags_Overwrites_Previous_Tags ───────────────────────────────

    [Test]
    public void SetTags_Overwrites_Previous_Tags()
    {
        var client = CreateBlob("tag-container", "overwrite.txt");

        client.SetTags(new Dictionary<string, string>
        {
            ["key1"] = "value1",
            ["key2"] = "value2"
        });

        client.SetTags(new Dictionary<string, string>
        {
            ["key3"] = "value3"
        });

        var result = client.GetTags().Value.Tags;
        Assert.That(result.ContainsKey("key1"), Is.False);
        Assert.That(result.ContainsKey("key2"), Is.False);
        Assert.That(result["key3"], Is.EqualTo("value3"));
    }

    // ── GetTags_Returns_Empty_When_No_Tags ─────────────────────────────

    [Test]
    public void GetTags_Returns_Empty_When_No_Tags()
    {
        var client = CreateBlob("tag-container", "no-tags.txt");

        var result = client.GetTags().Value.Tags;
        Assert.That(result, Is.Empty);
    }

    // ── GetTags_Throws_404_For_Missing_Blob ────────────────────────────

    [Test]
    public void GetTags_Throws_404_For_Missing_Blob()
    {
        var container = _fixture.CreateBlobContainerClient("tag-container");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("does-not-exist.txt");

        var ex = Assert.Throws<RequestFailedException>(() => client.GetTags());
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    // ── SetTags_Throws_404_For_Missing_Blob ────────────────────────────

    [Test]
    public void SetTags_Throws_404_For_Missing_Blob()
    {
        var container = _fixture.CreateBlobContainerClient("tag-container");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("does-not-exist.txt");

        var ex = Assert.Throws<RequestFailedException>(() =>
            client.SetTags(new Dictionary<string, string> { ["key"] = "value" }));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }
}
