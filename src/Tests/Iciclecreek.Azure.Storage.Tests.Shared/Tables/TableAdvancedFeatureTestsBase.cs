using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Tables;

[TestFixture]
public abstract class TableAdvancedFeatureTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void Setup() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── GetEntityIfExists_Returns_Null_For_Missing ──────────────────────

    [Test]
    public async Task GetEntityIfExists_Returns_Null_For_Missing()
    {
        var client = _fixture.CreateTableClient("geif-test");
        await client.CreateIfNotExistsAsync();

        var result = await client.GetEntityIfExistsAsync<TableEntity>("pk", "missing");
        Assert.That(result, Is.Null);
    }

    // ── GetEntityIfExists_Returns_Value_For_Existing ────────────────────

    [Test]
    public async Task GetEntityIfExists_Returns_Value_For_Existing()
    {
        var client = _fixture.CreateTableClient("geif-exists");
        await client.CreateIfNotExistsAsync();
        await client.AddEntityAsync(new TableEntity("pk", "rk") { ["V"] = "found" });

        var result = await client.GetEntityIfExistsAsync<TableEntity>("pk", "rk");
        Assert.That(result.HasValue, Is.True);
        Assert.That(result.Value!["V"]?.ToString(), Is.EqualTo("found"));
    }

    // ── ServiceClient_Query_FormattableString ───────────────────────────

    [Test]
    public void ServiceClient_Query_FormattableString()
    {
        var service = _fixture.CreateTableServiceClient();
        service.CreateTable("alpha");
        service.CreateTable("beta");

        var tableName = "alpha";
        FormattableString filter = $"TableName eq '{tableName}'";
        var all = service.Query(filter).ToList();
        Assert.That(all.Count, Is.GreaterThanOrEqualTo(1));
    }

    // ── Table_SetAccessPolicy_NoOp ──────────────────────────────────────

    [Test]
    public void Table_SetAccessPolicy_NoOp()
    {
        var client = _fixture.CreateTableClient("ns-table");
        client.CreateIfNotExists();
        Assert.DoesNotThrow(() =>
            client.SetAccessPolicy(Enumerable.Empty<TableSignedIdentifier>()));
    }

    // ── TableService_GetProperties_Returns_Default ──────────────────────

    [Test]
    public void TableService_GetProperties_Returns_Default()
    {
        var service = _fixture.CreateTableServiceClient();
        var props = service.GetProperties().Value;
        Assert.That(props, Is.Not.Null);
    }
}
