using Azure;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobContainerCrudTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    [Test]
    public void Create_Creates_Directory_On_Disk()
    {
        var client = FileBlobContainerClient.FromAccount(_root.Account, "my-container");
        client.Create();
        Assert.That(Directory.Exists(Path.Combine(_root.Account.BlobsRootPath, "my-container")), Is.True);
    }

    [Test]
    public void Create_Throws_If_Already_Exists()
    {
        var client = FileBlobContainerClient.FromAccount(_root.Account, "dupe");
        client.Create();
        Assert.Throws<RequestFailedException>(() => client.Create());
    }

    [Test]
    public void CreateIfNotExists_Is_Idempotent()
    {
        var client = FileBlobContainerClient.FromAccount(_root.Account, "idem");
        client.CreateIfNotExists();
        client.CreateIfNotExists();
        Assert.That(client.Exists().Value, Is.True);
    }

    [Test]
    public void Delete_Removes_Directory()
    {
        var client = FileBlobContainerClient.FromAccount(_root.Account, "deleteme");
        client.Create();
        Assert.That(client.Exists().Value, Is.True);
        client.Delete();
        Assert.That(client.Exists().Value, Is.False);
    }

    [Test]
    public void DeleteIfExists_Returns_False_When_Missing()
    {
        var client = FileBlobContainerClient.FromAccount(_root.Account, "ghost");
        Assert.That(client.DeleteIfExists().Value, Is.False);
    }

    [Test]
    public void ServiceClient_GetBlobContainers_Lists_Created_Containers()
    {
        var service = FileBlobServiceClient.FromAccount(_root.Account);
        service.CreateBlobContainer("one");
        service.CreateBlobContainer("two");

        var containers = service.GetBlobContainers().Select(c => c.Name).OrderBy(n => n).ToArray();
        Assert.That(containers, Is.EqualTo(new[] { "one", "two" }));
    }
}
