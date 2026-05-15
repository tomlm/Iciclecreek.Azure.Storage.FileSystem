using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Provider;

public class FileStorageProviderTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    [Test]
    public void AddAccount_Creates_Subdirectories()
    {
        Assert.That(Directory.Exists(_root.Account.BlobsRootPath), Is.True);
        Assert.That(Directory.Exists(_root.Account.TablesRootPath), Is.True);
    }

    [Test]
    public void GetAccount_Throws_For_Unknown()
    {
        Assert.Throws<InvalidOperationException>(() => _root.Provider.GetAccount("nonexistent"));
    }

    [Test]
    public void TryGetAccount_Returns_False_For_Unknown()
    {
        Assert.That(_root.Provider.TryGetAccount("nonexistent", out _), Is.False);
    }

    [Test]
    public void ConnectionString_Resolves_To_Account()
    {
        var connStr = _root.Account.GetConnectionString();

        // Should be able to construct clients from the connection string.
        var blobService = new FileBlobServiceClient(connStr, _root.Provider);
        Assert.That(blobService.AccountName, Is.EqualTo("testacct"));

        var tableService = new FileTableServiceClient(connStr, _root.Provider);
        Assert.That(tableService.AccountName, Is.EqualTo("testacct"));
    }

    [Test]
    public void Provider_Rehydrates_Existing_Accounts()
    {
        // Create a new provider pointing at the same root — should discover existing accounts.
        var provider2 = new FileStorageProvider(_root.Path);
        Assert.That(provider2.TryGetAccount("testacct", out _), Is.True);
    }
}
