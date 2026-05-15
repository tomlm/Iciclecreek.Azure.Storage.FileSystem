using Azure;
using Iciclecreek.Azure.Storage.SQLite.Blobs;
using Iciclecreek.Azure.Storage.SQLite.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.SQLite.Tests.Blobs;

public class BlobContainerCrudTests
{
    private TempDb _db = null!;

    [SetUp]
    public void Setup() => _db = new TempDb();

    [TearDown]
    public void TearDown() => _db.Dispose();

    [Test]
    public void Create_And_Exists()
    {
        var client = SqliteBlobContainerClient.FromAccount(_db.Account, "my-container");
        client.Create();
        Assert.That(client.Exists().Value, Is.True);
    }

    [Test]
    public void Create_Throws_If_Already_Exists()
    {
        var client = SqliteBlobContainerClient.FromAccount(_db.Account, "dupe");
        client.Create();
        Assert.Throws<RequestFailedException>(() => client.Create());
    }

    [Test]
    public void CreateIfNotExists_Is_Idempotent()
    {
        var client = SqliteBlobContainerClient.FromAccount(_db.Account, "idem");
        client.CreateIfNotExists();
        client.CreateIfNotExists();
        Assert.That(client.Exists().Value, Is.True);
    }

    [Test]
    public void Delete_Removes_Container()
    {
        var client = SqliteBlobContainerClient.FromAccount(_db.Account, "deleteme");
        client.Create();
        Assert.That(client.Exists().Value, Is.True);
        client.Delete();
        Assert.That(client.Exists().Value, Is.False);
    }

    [Test]
    public void DeleteIfExists_Returns_False_When_Missing()
    {
        var client = SqliteBlobContainerClient.FromAccount(_db.Account, "ghost");
        Assert.That(client.DeleteIfExists().Value, Is.False);
    }

    [Test]
    public void Exists_Returns_False_For_Missing_Container()
    {
        var client = SqliteBlobContainerClient.FromAccount(_db.Account, "nope");
        Assert.That(client.Exists().Value, Is.False);
    }

    [Test]
    public void ServiceClient_GetBlobContainers_Lists_Created_Containers()
    {
        var service = SqliteBlobServiceClient.FromAccount(_db.Account);
        service.CreateBlobContainer("one");
        service.CreateBlobContainer("two");

        var containers = service.GetBlobContainers().Select(c => c.Name).OrderBy(n => n).ToArray();
        Assert.That(containers, Is.EqualTo(new[] { "one", "two" }));
    }

    [Test]
    public void SetMetadata_And_GetProperties_Roundtrip()
    {
        var client = SqliteBlobContainerClient.FromAccount(_db.Account, "meta-container");
        client.Create();

        client.SetMetadata(new Dictionary<string, string> { ["env"] = "test", ["team"] = "dev" });

        var props = client.GetProperties().Value;
        Assert.That(props.Metadata["env"], Is.EqualTo("test"));
        Assert.That(props.Metadata["team"], Is.EqualTo("dev"));
    }

    [Test]
    public void DeleteIfExists_Returns_True_And_Removes()
    {
        var client = SqliteBlobContainerClient.FromAccount(_db.Account, "del-exists");
        client.Create();
        Assert.That(client.DeleteIfExists().Value, Is.True);
        Assert.That(client.Exists().Value, Is.False);
    }

    // ───────────────────── Async counterparts ─────────────────────

    [Test]
    public async Task Create_And_Exists_Async()
    {
        var client = SqliteBlobContainerClient.FromAccount(_db.Account, "my-container");
        await client.CreateAsync();
        Assert.That((await client.ExistsAsync()).Value, Is.True);
    }

    [Test]
    public void Create_Throws_If_Already_Exists_Async()
    {
        var client = SqliteBlobContainerClient.FromAccount(_db.Account, "dupe");
        client.Create();
        Assert.ThrowsAsync<RequestFailedException>(async () => await client.CreateAsync());
    }

    [Test]
    public async Task CreateIfNotExists_Is_Idempotent_Async()
    {
        var client = SqliteBlobContainerClient.FromAccount(_db.Account, "idem");
        await client.CreateIfNotExistsAsync();
        await client.CreateIfNotExistsAsync();
        Assert.That((await client.ExistsAsync()).Value, Is.True);
    }

    [Test]
    public async Task Delete_Removes_Container_Async()
    {
        var client = SqliteBlobContainerClient.FromAccount(_db.Account, "deleteme");
        await client.CreateAsync();
        Assert.That((await client.ExistsAsync()).Value, Is.True);
        await client.DeleteAsync();
        Assert.That((await client.ExistsAsync()).Value, Is.False);
    }

    [Test]
    public async Task DeleteIfExists_Returns_False_When_Missing_Async()
    {
        var client = SqliteBlobContainerClient.FromAccount(_db.Account, "ghost");
        Assert.That((await client.DeleteIfExistsAsync()).Value, Is.False);
    }

    [Test]
    public async Task ServiceClient_GetBlobContainers_Lists_Created_Containers_Async()
    {
        var service = SqliteBlobServiceClient.FromAccount(_db.Account);
        await service.CreateBlobContainerAsync("one");
        await service.CreateBlobContainerAsync("two");

        var names = new List<string>();
        await foreach (var c in service.GetBlobContainersAsync())
            names.Add(c.Name);

        names.Sort();
        Assert.That(names.ToArray(), Is.EqualTo(new[] { "one", "two" }));
    }
}
