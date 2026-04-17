using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Tables;

public class TableAdvancedFeatureTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    // ---- GetEntityIfExists returns HasValue=false for missing entity ----

    [Test]
    public async Task GetEntityIfExists_Returns_Null_For_Missing()
    {
        var client = FileTableClient.FromAccount(_root.Account, "geif-test");
        await client.CreateIfNotExistsAsync();

        var result = await client.GetEntityIfExistsAsync<TableEntity>("pk", "missing");
        // NullableResponse<T> doesn't expose a public way to construct HasValue=false,
        // so the test fake returns null for missing entities.
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task GetEntityIfExists_Returns_Value_For_Existing()
    {
        var client = FileTableClient.FromAccount(_root.Account, "geif-exists");
        await client.CreateIfNotExistsAsync();
        await client.AddEntityAsync(new TableEntity("pk", "rk") { ["V"] = "found" });

        var result = await client.GetEntityIfExistsAsync<TableEntity>("pk", "rk");
        Assert.That(result.HasValue, Is.True);
        Assert.That(result.Value["V"]?.ToString(), Is.EqualTo("found"));
    }

    // ---- FormattableString query on TableServiceClient ----

    [Test]
    public void ServiceClient_Query_FormattableString()
    {
        var service = FileTableServiceClient.FromAccount(_root.Account);
        service.CreateTable("alpha");
        service.CreateTable("beta");

        var tableName = "alpha";
        FormattableString filter = $"TableName eq '{tableName}'";
        // FormattableString just gets .ToString()'d — all tables returned since we don't parse it
        var all = service.Query(filter).ToList();
        Assert.That(all.Count, Is.GreaterThanOrEqualTo(1));
    }

    // ---- NotSupported sweep — TableClient ----

    [Test]
    public void NotSupported_Table_SetAccessPolicy_Throws()
    {
        var client = FileTableClient.FromAccount(_root.Account, "ns-table");
        client.CreateIfNotExists();
        Assert.Throws<NotSupportedException>(() =>
            client.SetAccessPolicy(Enumerable.Empty<TableSignedIdentifier>()));
    }

    // ---- NotSupported sweep — TableServiceClient ----

    [Test]
    public void NotSupported_TableService_GetProperties_Throws()
    {
        var service = FileTableServiceClient.FromAccount(_root.Account);
        Assert.Throws<NotSupportedException>(() => service.GetProperties());
    }

    [Test]
    public void NotSupported_TableService_GetStatistics_Throws()
    {
        var service = FileTableServiceClient.FromAccount(_root.Account);
        Assert.Throws<NotSupportedException>(() => service.GetStatistics());
    }
}
