using Azure;
using Azure.Storage.Queues;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Queues;

[TestFixture]
public abstract class QueueCrudTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void Setup() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── CreateIfNotExists_Is_Idempotent ─────────────────────────────────

    [Test]
    public void CreateIfNotExists_Is_Idempotent()
    {
        var client = _fixture.CreateQueueClient("idempotent-q");
        client.CreateIfNotExists();
        Assert.DoesNotThrow(() => client.CreateIfNotExists());
    }

    // ── Exists_Returns_False_For_Missing_Queue ──────────────────────────

    [Test]
    public void Exists_Returns_False_For_Missing_Queue()
    {
        var client = _fixture.CreateQueueClient("nope");
        Assert.That(client.Exists().Value, Is.False);
    }

    // ── Exists_Returns_True_After_Create ────────────────────────────────

    [Test]
    public void Exists_Returns_True_After_Create()
    {
        var client = _fixture.CreateQueueClient("exists-q");
        client.Create();
        Assert.That(client.Exists().Value, Is.True);
    }

    // ── Delete_Throws_404_When_Missing ──────────────────────────────────

    [Test]
    public void Delete_Throws_404_When_Missing()
    {
        var client = _fixture.CreateQueueClient("missing-q");
        var ex = Assert.Throws<RequestFailedException>(() => client.Delete());
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    // ── DeleteIfExists_Returns_False_When_Missing ───────────────────────

    [Test]
    public void DeleteIfExists_Returns_False_When_Missing()
    {
        var client = _fixture.CreateQueueClient("missing-q");
        Assert.That(client.DeleteIfExists().Value, Is.False);
    }

    // ── DeleteIfExists_Returns_True_And_Deletes ─────────────────────────

    [Test]
    public void DeleteIfExists_Returns_True_And_Deletes()
    {
        var client = _fixture.CreateQueueClient("del-q2");
        client.Create();
        Assert.That(client.DeleteIfExists().Value, Is.True);
        Assert.That(client.Exists().Value, Is.False);
    }

    // ── Name_Returns_Queue_Name ─────────────────────────────────────────

    [Test]
    public void Name_Returns_Queue_Name()
    {
        var client = _fixture.CreateQueueClient("named-q");
        Assert.That(client.Name, Is.EqualTo("named-q"));
    }

    // ── SetMetadata_And_GetProperties_Roundtrip ─────────────────────────

    [Test]
    public void SetMetadata_And_GetProperties_Roundtrip()
    {
        var client = _fixture.CreateQueueClient("meta-q");
        client.Create();

        client.SetMetadata(new Dictionary<string, string> { ["env"] = "test", ["region"] = "us" });

        var props = client.GetProperties().Value;
        Assert.That(props.Metadata["env"], Is.EqualTo("test"));
        Assert.That(props.Metadata["region"], Is.EqualTo("us"));
    }

    // ── GetProperties_Returns_ApproximateMessageCount ────────────────────

    [Test]
    public void GetProperties_Returns_ApproximateMessageCount()
    {
        var client = _fixture.CreateQueueClient("count-q");
        client.Create();

        client.SendMessage("msg1");
        client.SendMessage("msg2");

        var props = client.GetProperties().Value;
        Assert.That(props.ApproximateMessagesCount, Is.EqualTo(2));
    }

    // ── GetProperties_Throws_404_When_Missing ───────────────────────────

    [Test]
    public void GetProperties_Throws_404_When_Missing()
    {
        var client = _fixture.CreateQueueClient("missing-q");
        var ex = Assert.Throws<RequestFailedException>(() => client.GetProperties());
        Assert.That(ex!.Status, Is.EqualTo(404));
    }
}
