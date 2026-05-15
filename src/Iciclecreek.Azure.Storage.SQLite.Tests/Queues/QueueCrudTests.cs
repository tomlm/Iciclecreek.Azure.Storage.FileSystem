using Azure;
using Azure.Storage.Queues;
using Iciclecreek.Azure.Storage.SQLite.Queues;
using Iciclecreek.Azure.Storage.SQLite.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.SQLite.Tests.Queues;

public class QueueCrudTests
{
    private TempDb _db = null!;

    [SetUp]
    public void Setup() => _db = new TempDb();

    [TearDown]
    public void TearDown() => _db.Dispose();

    // ── Queue Create / Exists / Delete ──────────────────────────────────

    [Test]
    public void Create_And_Exists()
    {
        var client = SqliteQueueClient.FromAccount(_db.Account, "my-queue");
        client.Create();
        Assert.That(client.Exists().Value, Is.True);
    }

    [Test]
    public async Task CreateAsync_And_ExistsAsync()
    {
        var client = SqliteQueueClient.FromAccount(_db.Account, "my-queue-async");
        await client.CreateAsync();
        Assert.That((await client.ExistsAsync()).Value, Is.True);
    }

    [Test]
    public void CreateIfNotExists_Is_Idempotent()
    {
        var client = SqliteQueueClient.FromAccount(_db.Account, "idempotent-q");
        client.CreateIfNotExists();
        Assert.DoesNotThrow(() => client.CreateIfNotExists());
    }

    [Test]
    public void Exists_Returns_False_For_Missing_Queue()
    {
        var client = SqliteQueueClient.FromAccount(_db.Account, "nope");
        Assert.That(client.Exists().Value, Is.False);
    }

    [Test]
    public void Exists_Returns_True_After_Create()
    {
        var client = SqliteQueueClient.FromAccount(_db.Account, "exists-q");
        client.Create();
        Assert.That(client.Exists().Value, Is.True);
    }

    [Test]
    public void Delete_Removes_Queue()
    {
        var client = SqliteQueueClient.FromAccount(_db.Account, "del-q");
        client.Create();
        client.Delete();
        Assert.That(client.Exists().Value, Is.False);
    }

    [Test]
    public void Delete_Throws_404_When_Missing()
    {
        var client = SqliteQueueClient.FromAccount(_db.Account, "missing-q");
        var ex = Assert.Throws<RequestFailedException>(() => client.Delete());
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    [Test]
    public void DeleteIfExists_Returns_False_When_Missing()
    {
        var client = SqliteQueueClient.FromAccount(_db.Account, "missing-q");
        Assert.That(client.DeleteIfExists().Value, Is.False);
    }

    [Test]
    public void DeleteIfExists_Returns_True_And_Deletes()
    {
        var client = SqliteQueueClient.FromAccount(_db.Account, "del-q2");
        client.Create();
        Assert.That(client.DeleteIfExists().Value, Is.True);
        Assert.That(client.Exists().Value, Is.False);
    }

    // ── Properties ──────────────────────────────────────────────────────

    [Test]
    public void Name_Returns_Queue_Name()
    {
        var client = SqliteQueueClient.FromAccount(_db.Account, "named-q");
        Assert.That(client.Name, Is.EqualTo("named-q"));
    }

    [Test]
    public void AccountName_Returns_Account_Name()
    {
        var client = SqliteQueueClient.FromAccount(_db.Account, "named-q");
        Assert.That(client.AccountName, Is.EqualTo(_db.Account.Name));
    }

    // ── Metadata ────────────────────────────────────────────────────────

    [Test]
    public void SetMetadata_And_GetProperties_Roundtrip()
    {
        var client = SqliteQueueClient.FromAccount(_db.Account, "meta-q");
        client.Create();

        client.SetMetadata(new Dictionary<string, string> { ["env"] = "test", ["region"] = "us" });

        var props = client.GetProperties().Value;
        Assert.That(props.Metadata["env"], Is.EqualTo("test"));
        Assert.That(props.Metadata["region"], Is.EqualTo("us"));
    }

    [Test]
    public void GetProperties_Returns_ApproximateMessageCount()
    {
        var client = SqliteQueueClient.FromAccount(_db.Account, "count-q");
        client.Create();

        client.SendMessage("msg1");
        client.SendMessage("msg2");

        var props = client.GetProperties().Value;
        Assert.That(props.ApproximateMessagesCount, Is.EqualTo(2));
    }

    [Test]
    public void GetProperties_Throws_404_When_Missing()
    {
        var client = SqliteQueueClient.FromAccount(_db.Account, "missing-q");
        var ex = Assert.Throws<RequestFailedException>(() => client.GetProperties());
        Assert.That(ex!.Status, Is.EqualTo(404));
    }
}
