using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Tables;

[TestFixture]
public abstract class TableQueryTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void Setup() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── Helpers ─────────────────────────────────────────────────────────

    protected TableClient SetupPeopleTable()
    {
        var client = _fixture.CreateTableClient("people");
        client.CreateIfNotExists();
        client.AddEntity(new TableEntity("users", "alice") { ["Name"] = "Alice", ["Age"] = 30 });
        client.AddEntity(new TableEntity("users", "bob") { ["Name"] = "Bob", ["Age"] = 25 });
        client.AddEntity(new TableEntity("admins", "carol") { ["Name"] = "Carol", ["Age"] = 40 });
        return client;
    }

    protected async Task<TableClient> SetupPeopleTableAsync()
    {
        var client = _fixture.CreateTableClient("people");
        await client.CreateIfNotExistsAsync();
        await client.AddEntityAsync(new TableEntity("users", "alice") { ["Name"] = "Alice", ["Age"] = 30 });
        await client.AddEntityAsync(new TableEntity("users", "bob") { ["Name"] = "Bob", ["Age"] = 25 });
        await client.AddEntityAsync(new TableEntity("admins", "carol") { ["Name"] = "Carol", ["Age"] = 40 });
        return client;
    }

    // ── Query_All_Returns_All_Entities ──────────────────────────────────

    [Test]
    public void Query_All_Returns_All_Entities()
    {
        var client = SetupPeopleTable();
        var results = client.Query<TableEntity>().ToList();
        Assert.That(results, Has.Count.EqualTo(3));
    }

    [Test]
    public async Task Query_All_Returns_All_Entities_Async()
    {
        var client = await SetupPeopleTableAsync();
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>()) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(3));
    }

    // ── Query_String_Filter_PartitionKey_Eq ─────────────────────────────

    [Test]
    public void Query_String_Filter_PartitionKey_Eq()
    {
        var client = SetupPeopleTable();
        var results = client.Query<TableEntity>("PartitionKey eq 'users'").ToList();
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results.All(r => r.PartitionKey == "users"), Is.True);
    }

    [Test]
    public async Task Query_String_Filter_PartitionKey_Eq_Async()
    {
        var client = await SetupPeopleTableAsync();
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>("PartitionKey eq 'users'")) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results.All(r => r.PartitionKey == "users"), Is.True);
    }

    // ── Query_String_Filter_And ─────────────────────────────────────────

    [Test]
    public void Query_String_Filter_And()
    {
        var client = SetupPeopleTable();
        var results = client.Query<TableEntity>("PartitionKey eq 'users' and RowKey eq 'alice'").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0]["Name"]?.ToString(), Is.EqualTo("Alice"));
    }

    [Test]
    public async Task Query_String_Filter_And_Async()
    {
        var client = await SetupPeopleTableAsync();
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>("PartitionKey eq 'users' and RowKey eq 'alice'")) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0]["Name"]?.ToString(), Is.EqualTo("Alice"));
    }

    // ── Query_String_Filter_Int_Comparison ──────────────────────────────

    [Test]
    public void Query_String_Filter_Int_Comparison()
    {
        var client = SetupPeopleTable();
        var results = client.Query<TableEntity>("Age gt 28").ToList();
        Assert.That(results, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task Query_String_Filter_Int_Comparison_Async()
    {
        var client = await SetupPeopleTableAsync();
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>("Age gt 28")) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(2));
    }

    // ── Query_Linq_Filter ───────────────────────────────────────────────

    [Test]
    public void Query_Linq_Filter()
    {
        var client = SetupPeopleTable();
        var results = client.Query<TableEntity>(e => e.PartitionKey == "admins").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0]["Name"]?.ToString(), Is.EqualTo("Carol"));
    }

    [Test]
    public async Task Query_Linq_Filter_Async()
    {
        var client = await SetupPeopleTableAsync();
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>(e => e.PartitionKey == "admins")) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0]["Name"]?.ToString(), Is.EqualTo("Carol"));
    }

    // ── ServiceClient_Lists_Tables ──────────────────────────────────────

    [Test]
    public void ServiceClient_Lists_Tables()
    {
        var service = _fixture.CreateTableServiceClient();
        service.CreateTable("alpha");
        service.CreateTable("beta");

        var names = service.Query().Select(t => t.Name).OrderBy(n => n).ToArray();
        Assert.That(names, Is.EqualTo(new[] { "alpha", "beta" }));
    }

    [Test]
    public async Task ServiceClient_Lists_Tables_Async()
    {
        var service = _fixture.CreateTableServiceClient();
        service.CreateTable("alpha");
        service.CreateTable("beta");

        var names = new List<string>();
        await foreach (var t in service.QueryAsync()) names.Add(t.Name);
        names.Sort();
        Assert.That(names.ToArray(), Is.EqualTo(new[] { "alpha", "beta" }));
    }
}
