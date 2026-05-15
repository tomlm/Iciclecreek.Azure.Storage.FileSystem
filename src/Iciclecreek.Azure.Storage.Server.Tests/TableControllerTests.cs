using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace Iciclecreek.Azure.Storage.Server.Tests;

[TestFixture]
public class TableControllerTests
{
    private HttpClient Http => StorageServerFixture.TableHttp;
    private const string Account = StorageServerFixture.AccountName;
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = null };

    // ── Table CRUD ──────────────────────────────────────────────────────

    [Test]
    public async Task CreateTable_Returns201()
    {
        var body = JsonSerializer.Serialize(new { TableName = "TestCreateTable" }, JsonOpts);
        var resp = await Http.PostAsync($"/{Account}/Tables",
            new StringContent(body, Encoding.UTF8, "application/json"));

        Assert.That(resp.StatusCode, Is.EqualTo(HttpStatusCode.Created));
        var json = await resp.Content.ReadAsStringAsync();
        Assert.That(json, Does.Contain("TestCreateTable"));
    }

    [Test]
    public async Task QueryTables_ReturnsCreatedTables()
    {
        // Create tables
        await PostTable("QueryTablesA");
        await PostTable("QueryTablesB");

        var resp = await Http.GetAsync($"/{Account}/Tables");
        if (!resp.IsSuccessStatusCode)
        {
            var errBody = await resp.Content.ReadAsStringAsync();
            Assert.Fail($"Expected OK but got {resp.StatusCode}: {errBody}");
        }

        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        var tableNames = doc.RootElement
            .GetProperty("value")
            .EnumerateArray()
            .Select(e => e.GetProperty("TableName").GetString())
            .ToList();

        Assert.That(tableNames, Does.Contain("QueryTablesA"));
        Assert.That(tableNames, Does.Contain("QueryTablesB"));
    }

    [Test]
    public async Task DeleteTable_Returns204()
    {
        await PostTable("TableToDelete");

        var resp = await Http.DeleteAsync($"/{Account}/Tables('TableToDelete')");
        Assert.That(resp.StatusCode, Is.EqualTo(HttpStatusCode.NoContent));
    }

    // ── Entity Insert and Get ───────────────────────────────────────────

    [Test]
    public async Task InsertEntity_Returns201_WithETag()
    {
        await PostTable("InsertEntityTest");

        var entity = new { PartitionKey = "pk1", RowKey = "rk1", Name = "Alice", Age = 30 };
        var resp = await PostEntity("InsertEntityTest", entity);

        Assert.That(resp.StatusCode, Is.EqualTo(HttpStatusCode.Created));
        Assert.That(resp.Headers.Contains("ETag"), Is.True);

        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        Assert.That(doc.RootElement.GetProperty("PartitionKey").GetString(), Is.EqualTo("pk1"));
        Assert.That(doc.RootElement.GetProperty("RowKey").GetString(), Is.EqualTo("rk1"));
        Assert.That(doc.RootElement.GetProperty("Name").GetString(), Is.EqualTo("Alice"));
        Assert.That(doc.RootElement.GetProperty("Age").GetInt32(), Is.EqualTo(30));
    }

    [Test]
    public async Task GetEntity_ReturnsSingleEntity()
    {
        await PostTable("GetEntityTest");
        await PostEntity("GetEntityTest", new { PartitionKey = "users", RowKey = "u1", Email = "test@test.com" });

        var resp = await Http.GetAsync(
            $"/{Account}/GetEntityTest(PartitionKey='users',RowKey='u1')");
        Assert.That(resp.StatusCode, Is.EqualTo(HttpStatusCode.OK));

        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        Assert.That(doc.RootElement.GetProperty("Email").GetString(), Is.EqualTo("test@test.com"));
    }

    // ── Query Entities ──────────────────────────────────────────────────

    [Test]
    public async Task QueryEntities_ReturnsAllEntities()
    {
        await PostTable("QueryAllTest");
        await PostEntity("QueryAllTest", new { PartitionKey = "p", RowKey = "r1", Val = 1 });
        await PostEntity("QueryAllTest", new { PartitionKey = "p", RowKey = "r2", Val = 2 });
        await PostEntity("QueryAllTest", new { PartitionKey = "p", RowKey = "r3", Val = 3 });

        var resp = await Http.GetAsync($"/{Account}/QueryAllTest()");
        Assert.That(resp.StatusCode, Is.EqualTo(HttpStatusCode.OK));

        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        var count = doc.RootElement.GetProperty("value").GetArrayLength();
        Assert.That(count, Is.EqualTo(3));
    }

    [Test]
    public async Task QueryEntities_WithFilter_FiltersResults()
    {
        await PostTable("QueryFilterTest");
        await PostEntity("QueryFilterTest", new { PartitionKey = "p", RowKey = "r1", Score = 10 });
        await PostEntity("QueryFilterTest", new { PartitionKey = "p", RowKey = "r2", Score = 50 });
        await PostEntity("QueryFilterTest", new { PartitionKey = "p", RowKey = "r3", Score = 90 });

        var filter = Uri.EscapeDataString("Score gt 40");
        var resp = await Http.GetAsync($"/{Account}/QueryFilterTest()?$filter={filter}");
        Assert.That(resp.StatusCode, Is.EqualTo(HttpStatusCode.OK));

        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        var count = doc.RootElement.GetProperty("value").GetArrayLength();
        Assert.That(count, Is.EqualTo(2));
    }

    // ── Update Entity (Replace) ─────────────────────────────────────────

    [Test]
    public async Task UpdateEntity_Replace_ChangesAllProperties()
    {
        await PostTable("UpdateReplaceTest");
        await PostEntity("UpdateReplaceTest", new { PartitionKey = "p", RowKey = "r1", Name = "Original", Extra = "keep" });

        // Replace — only Name, no Extra
        var updated = JsonSerializer.Serialize(new { Name = "Updated" }, JsonOpts);
        var req = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/UpdateReplaceTest(PartitionKey='p',RowKey='r1')")
        {
            Content = new StringContent(updated, Encoding.UTF8, "application/json")
        };
        req.Headers.Add("If-Match", "*");
        var resp = await Http.SendAsync(req);
        Assert.That(resp.StatusCode, Is.EqualTo(HttpStatusCode.NoContent));

        // Verify
        var getResp = await Http.GetAsync(
            $"/{Account}/UpdateReplaceTest(PartitionKey='p',RowKey='r1')");
        var json = await getResp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        Assert.That(doc.RootElement.GetProperty("Name").GetString(), Is.EqualTo("Updated"));
        // Extra should be gone (replace mode)
        Assert.That(doc.RootElement.TryGetProperty("Extra", out _), Is.False);
    }

    // ── Merge Entity ────────────────────────────────────────────────────

    [Test]
    public async Task MergeEntity_PreservesExistingProperties()
    {
        await PostTable("MergeTest");
        await PostEntity("MergeTest", new { PartitionKey = "p", RowKey = "r1", Name = "Alice", Age = 30 });

        // Merge — update Name, keep Age
        var patch = JsonSerializer.Serialize(new { Name = "Bob" }, JsonOpts);
        var req = new HttpRequestMessage(new HttpMethod("MERGE"),
            $"/{Account}/MergeTest(PartitionKey='p',RowKey='r1')")
        {
            Content = new StringContent(patch, Encoding.UTF8, "application/json")
        };
        req.Headers.Add("If-Match", "*");
        var resp = await Http.SendAsync(req);
        Assert.That(resp.StatusCode, Is.EqualTo(HttpStatusCode.NoContent));

        // Verify merge
        var getResp = await Http.GetAsync(
            $"/{Account}/MergeTest(PartitionKey='p',RowKey='r1')");
        var json = await getResp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        Assert.That(doc.RootElement.GetProperty("Name").GetString(), Is.EqualTo("Bob"));
        Assert.That(doc.RootElement.GetProperty("Age").GetInt32(), Is.EqualTo(30));
    }

    // ── Delete Entity ───────────────────────────────────────────────────

    [Test]
    public async Task DeleteEntity_Returns204()
    {
        await PostTable("DeleteEntityTest");
        await PostEntity("DeleteEntityTest", new { PartitionKey = "p", RowKey = "r1", X = 1 });

        var req = new HttpRequestMessage(HttpMethod.Delete,
            $"/{Account}/DeleteEntityTest(PartitionKey='p',RowKey='r1')");
        req.Headers.Add("If-Match", "*");
        var resp = await Http.SendAsync(req);
        Assert.That(resp.StatusCode, Is.EqualTo(HttpStatusCode.NoContent));

        // Verify gone
        var getResp = await Http.GetAsync(
            $"/{Account}/DeleteEntityTest(PartitionKey='p',RowKey='r1')");
        Assert.That(getResp.StatusCode, Is.EqualTo(HttpStatusCode.NotFound));
    }

    // ── EDM Type Preservation ───────────────────────────────────────────

    [Test]
    public async Task EntityRoundtrip_PreservesEdmTypes()
    {
        await PostTable("EdmTypeTest");

        var body = JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["PartitionKey"] = "p",
            ["RowKey"] = "r1",
            ["Name"] = "Test",
            ["BigNumber"] = "9999999999",
            ["BigNumber@odata.type"] = "Edm.Int64",
            ["Id"] = "550e8400-e29b-41d4-a716-446655440000",
            ["Id@odata.type"] = "Edm.Guid",
        }, JsonOpts);

        await Http.PostAsync($"/{Account}/EdmTypeTest",
            new StringContent(body, Encoding.UTF8, "application/json"));

        var resp = await Http.GetAsync(
            $"/{Account}/EdmTypeTest(PartitionKey='p',RowKey='r1')");
        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);

        // Int64 should come back with type annotation
        Assert.That(doc.RootElement.GetProperty("BigNumber@odata.type").GetString(), Is.EqualTo("Edm.Int64"));
        Assert.That(doc.RootElement.GetProperty("BigNumber").GetString(), Is.EqualTo("9999999999"));

        // Guid should come back with type annotation
        Assert.That(doc.RootElement.GetProperty("Id@odata.type").GetString(), Is.EqualTo("Edm.Guid"));
    }

    // ── Multiple Partitions ─────────────────────────────────────────────

    [Test]
    public async Task QueryEntities_AcrossPartitions()
    {
        await PostTable("MultiPartTest");
        await PostEntity("MultiPartTest", new { PartitionKey = "dept-eng", RowKey = "e1", Name = "Alice" });
        await PostEntity("MultiPartTest", new { PartitionKey = "dept-sales", RowKey = "s1", Name = "Bob" });

        var resp = await Http.GetAsync($"/{Account}/MultiPartTest()");
        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        var count = doc.RootElement.GetProperty("value").GetArrayLength();
        Assert.That(count, Is.EqualTo(2));
    }

    // ── Response Headers ────────────────────────────────────────────────

    [Test]
    public async Task TableResponses_Include_StorageHeaders()
    {
        await PostTable("HeaderTest");

        // Use an endpoint that works (create table returns headers too)
        var body = JsonSerializer.Serialize(new { TableName = "HeaderTest2" }, JsonOpts);
        var resp = await Http.PostAsync($"/{Account}/Tables",
            new StringContent(body, Encoding.UTF8, "application/json"));

        Assert.That(resp.Headers.Contains("x-ms-version"), Is.True);
        Assert.That(resp.Headers.Contains("x-ms-request-id"), Is.True);
    }

    // ── $select ──────────────────────────────────────────────────────────

    [Test]
    public async Task QueryEntities_WithSelect_ReturnsOnlySelectedProperties()
    {
        await PostTable("SelectTest");
        await PostEntity("SelectTest", new { PartitionKey = "p", RowKey = "r1", Name = "Alice", Age = 30, City = "NYC" });

        var resp = await Http.GetAsync($"/{Account}/SelectTest()?$select=Name,City");
        Assert.That(resp.StatusCode, Is.EqualTo(HttpStatusCode.OK));

        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        var entity = doc.RootElement.GetProperty("value")[0];

        // Selected properties should be present
        Assert.That(entity.TryGetProperty("Name", out _), Is.True);
        Assert.That(entity.TryGetProperty("City", out _), Is.True);
        // Non-selected should be absent
        Assert.That(entity.TryGetProperty("Age", out _), Is.False);
        // System properties always present
        Assert.That(entity.TryGetProperty("PartitionKey", out _), Is.True);
        Assert.That(entity.TryGetProperty("RowKey", out _), Is.True);
    }

    // ── $top ────────────────────────────────────────────────────────────

    [Test]
    public async Task QueryEntities_WithTop_LimitsResults()
    {
        await PostTable("TopTest");
        await PostEntity("TopTest", new { PartitionKey = "p", RowKey = "r1", Val = 1 });
        await PostEntity("TopTest", new { PartitionKey = "p", RowKey = "r2", Val = 2 });
        await PostEntity("TopTest", new { PartitionKey = "p", RowKey = "r3", Val = 3 });
        await PostEntity("TopTest", new { PartitionKey = "p", RowKey = "r4", Val = 4 });

        var resp = await Http.GetAsync($"/{Account}/TopTest()?$top=2");
        Assert.That(resp.StatusCode, Is.EqualTo(HttpStatusCode.OK));

        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        var count = doc.RootElement.GetProperty("value").GetArrayLength();
        Assert.That(count, Is.EqualTo(2));
    }

    // ── $filter + $top + $select combined ───────────────────────────────

    [Test]
    public async Task QueryEntities_CombinedOptions()
    {
        await PostTable("CombinedTest");
        await PostEntity("CombinedTest", new { PartitionKey = "p", RowKey = "r1", Score = 10, Name = "Low" });
        await PostEntity("CombinedTest", new { PartitionKey = "p", RowKey = "r2", Score = 50, Name = "Mid" });
        await PostEntity("CombinedTest", new { PartitionKey = "p", RowKey = "r3", Score = 90, Name = "High" });
        await PostEntity("CombinedTest", new { PartitionKey = "p", RowKey = "r4", Score = 80, Name = "AlsoHigh" });

        var filter = Uri.EscapeDataString("Score gt 40");
        var resp = await Http.GetAsync($"/{Account}/CombinedTest()?$filter={filter}&$top=2&$select=Name");
        Assert.That(resp.StatusCode, Is.EqualTo(HttpStatusCode.OK));

        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        var values = doc.RootElement.GetProperty("value");
        Assert.That(values.GetArrayLength(), Is.EqualTo(2));
        // Only Name should be in custom properties
        var first = values[0];
        Assert.That(first.TryGetProperty("Name", out _), Is.True);
        Assert.That(first.TryGetProperty("Score", out _), Is.False);
    }

    // ── $batch ──────────────────────────────────────────────────────────

    [Test]
    public async Task Batch_InsertsMultipleEntities()
    {
        await PostTable("BatchInsert");

        var boundary = $"batch_{Guid.NewGuid()}";
        var batchBody = new StringBuilder();
        for (int i = 1; i <= 3; i++)
        {
            batchBody.AppendLine($"--{boundary}");
            batchBody.AppendLine("Content-Type: application/http");
            batchBody.AppendLine("Content-Transfer-Encoding: binary");
            batchBody.AppendLine();
            batchBody.AppendLine($"POST /{Account}/BatchInsert HTTP/1.1");
            batchBody.AppendLine("Content-Type: application/json");
            batchBody.AppendLine();
            batchBody.AppendLine($"{{\"PartitionKey\":\"pk\",\"RowKey\":\"rk{i}\",\"Value\":{i}}}");
        }
        batchBody.AppendLine($"--{boundary}--");

        var req = new HttpRequestMessage(HttpMethod.Post, $"/{Account}/$batch")
        {
            Content = new StringContent(batchBody.ToString(), Encoding.UTF8, "text/plain")
        };
        req.Content!.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("multipart/mixed");
        req.Content.Headers.ContentType.Parameters.Add(
            new System.Net.Http.Headers.NameValueHeaderValue("boundary", boundary));
        var resp = await Http.SendAsync(req);
        Assert.That((int)resp.StatusCode, Is.EqualTo(202));

        // Verify entities were created
        var queryResp = await Http.GetAsync($"/{Account}/BatchInsert()");
        var json = await queryResp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        Assert.That(doc.RootElement.GetProperty("value").GetArrayLength(), Is.EqualTo(3));
    }

    [Test]
    public async Task Batch_MixedOperations()
    {
        await PostTable("BatchMixed");
        // Pre-insert an entity to update/delete
        await PostEntity("BatchMixed", new { PartitionKey = "pk", RowKey = "existing", Name = "Original" });

        var boundary = $"batch_{Guid.NewGuid()}";
        var batchBody = new StringBuilder();

        // Insert new
        batchBody.AppendLine($"--{boundary}");
        batchBody.AppendLine("Content-Type: application/http");
        batchBody.AppendLine("Content-Transfer-Encoding: binary");
        batchBody.AppendLine();
        batchBody.AppendLine($"POST /{Account}/BatchMixed HTTP/1.1");
        batchBody.AppendLine("Content-Type: application/json");
        batchBody.AppendLine();
        batchBody.AppendLine("{\"PartitionKey\":\"pk\",\"RowKey\":\"new1\",\"Name\":\"New\"}");

        // Update existing (merge)
        batchBody.AppendLine($"--{boundary}");
        batchBody.AppendLine("Content-Type: application/http");
        batchBody.AppendLine("Content-Transfer-Encoding: binary");
        batchBody.AppendLine();
        batchBody.AppendLine($"MERGE /{Account}/BatchMixed(PartitionKey='pk',RowKey='existing') HTTP/1.1");
        batchBody.AppendLine("Content-Type: application/json");
        batchBody.AppendLine("If-Match: *");
        batchBody.AppendLine();
        batchBody.AppendLine("{\"Name\":\"Updated\"}");

        batchBody.AppendLine($"--{boundary}--");

        var req = new HttpRequestMessage(HttpMethod.Post, $"/{Account}/$batch")
        {
            Content = new StringContent(batchBody.ToString(), Encoding.UTF8, "text/plain")
        };
        req.Content!.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("multipart/mixed");
        req.Content.Headers.ContentType.Parameters.Add(
            new System.Net.Http.Headers.NameValueHeaderValue("boundary", boundary));
        var resp = await Http.SendAsync(req);
        Assert.That((int)resp.StatusCode, Is.EqualTo(202));

        // Verify results
        var getNew = await Http.GetAsync($"/{Account}/BatchMixed(PartitionKey='pk',RowKey='new1')");
        Assert.That(getNew.StatusCode, Is.EqualTo(HttpStatusCode.OK));

        var getUpdated = await Http.GetAsync($"/{Account}/BatchMixed(PartitionKey='pk',RowKey='existing')");
        var updatedJson = await getUpdated.Content.ReadAsStringAsync();
        using var updatedDoc = JsonDocument.Parse(updatedJson);
        Assert.That(updatedDoc.RootElement.GetProperty("Name").GetString(), Is.EqualTo("Updated"));
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private async Task PostTable(string tableName)
    {
        var body = JsonSerializer.Serialize(new { TableName = tableName }, JsonOpts);
        await Http.PostAsync($"/{Account}/Tables",
            new StringContent(body, Encoding.UTF8, "application/json"));
    }

    private async Task<HttpResponseMessage> PostEntity(string tableName, object entity)
    {
        var body = JsonSerializer.Serialize(entity, JsonOpts);
        return await Http.PostAsync($"/{Account}/{tableName}",
            new StringContent(body, Encoding.UTF8, "application/json"));
    }
}
