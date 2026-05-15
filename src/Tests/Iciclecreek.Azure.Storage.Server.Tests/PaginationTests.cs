using System.Net;
using System.Text;
using System.Text.Json;
using System.Xml.Linq;

namespace Iciclecreek.Azure.Storage.Server.Tests;

[TestFixture]
public class PaginationTests
{
    private HttpClient BlobHttp => StorageServerFixture.BlobHttp;
    private HttpClient TableHttp => StorageServerFixture.TableHttp;
    private HttpClient QueueHttp => StorageServerFixture.QueueHttp;
    private const string Account = StorageServerFixture.AccountName;

    // ── Blob Container Pagination ───────────────────────────────────────

    [Test]
    public async Task ListContainers_WithMaxResults_PaginatesCorrectly()
    {
        // Create 5 containers
        for (int i = 1; i <= 5; i++)
            await BlobHttp.SendAsync(new HttpRequestMessage(HttpMethod.Put,
                $"/{Account}/paginate-c{i:D2}?restype=container"));

        // Request first page of 2
        var resp1 = await BlobHttp.GetAsync($"/{Account}?comp=list&prefix=paginate-c&maxresults=2");
        Assert.That(resp1.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        var xml1 = XDocument.Parse(await resp1.Content.ReadAsStringAsync());
        var names1 = xml1.Descendants("Name").Select(e => e.Value).ToList();
        Assert.That(names1.Count, Is.EqualTo(2));

        var nextMarker = xml1.Descendants("NextMarker").First().Value;
        Assert.That(nextMarker, Is.Not.Empty, "Should have a NextMarker for more pages");

        // Request second page using marker
        var resp2 = await BlobHttp.GetAsync(
            $"/{Account}?comp=list&prefix=paginate-c&maxresults=2&marker={Uri.EscapeDataString(nextMarker)}");
        var xml2 = XDocument.Parse(await resp2.Content.ReadAsStringAsync());
        var names2 = xml2.Descendants("Name").Select(e => e.Value).ToList();
        Assert.That(names2.Count, Is.EqualTo(2));

        // Pages should not overlap
        Assert.That(names1.Intersect(names2).Count(), Is.EqualTo(0));
    }

    // ── Blob Listing Pagination ─────────────────────────────────────────

    [Test]
    public async Task ListBlobs_WithMaxResults_PaginatesCorrectly()
    {
        var containerName = "paginate-blobs";
        await BlobHttp.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        for (int i = 1; i <= 5; i++)
            await BlobHttp.SendAsync(new HttpRequestMessage(HttpMethod.Put,
                $"/{Account}/{containerName}/file{i:D2}.txt")
            { Content = new StringContent($"content{i}") });

        // First page of 2
        var resp1 = await BlobHttp.GetAsync(
            $"/{Account}/{containerName}?restype=container&comp=list&maxresults=2");
        var xml1 = XDocument.Parse(await resp1.Content.ReadAsStringAsync());
        var names1 = xml1.Descendants("Name").Select(e => e.Value).ToList();
        Assert.That(names1.Count, Is.EqualTo(2));

        var nextMarker = xml1.Descendants("NextMarker").First().Value;
        Assert.That(nextMarker, Is.Not.Empty);

        // Second page
        var resp2 = await BlobHttp.GetAsync(
            $"/{Account}/{containerName}?restype=container&comp=list&maxresults=2&marker={Uri.EscapeDataString(nextMarker)}");
        var xml2 = XDocument.Parse(await resp2.Content.ReadAsStringAsync());
        var names2 = xml2.Descendants("Name").Select(e => e.Value).ToList();
        Assert.That(names2.Count, Is.EqualTo(2));
        Assert.That(names1.Intersect(names2).Count(), Is.EqualTo(0));
    }

    // ── Queue Listing Pagination ────────────────────────────────────────

    [Test]
    public async Task ListQueues_WithMaxResults_PaginatesCorrectly()
    {
        for (int i = 1; i <= 4; i++)
            await QueueHttp.SendAsync(new HttpRequestMessage(HttpMethod.Put,
                $"/{Account}/paginateq-{i:D2}"));

        // First page of 2
        var resp1 = await QueueHttp.GetAsync(
            $"/{Account}?comp=list&prefix=paginateq-&maxresults=2");
        var xml1 = XDocument.Parse(await resp1.Content.ReadAsStringAsync());
        var names1 = xml1.Descendants("Name").Select(e => e.Value).ToList();
        Assert.That(names1.Count, Is.EqualTo(2));

        var nextMarker = xml1.Descendants("NextMarker").First().Value;
        Assert.That(nextMarker, Is.Not.Empty);

        // Second page
        var resp2 = await QueueHttp.GetAsync(
            $"/{Account}?comp=list&prefix=paginateq-&maxresults=2&marker={Uri.EscapeDataString(nextMarker)}");
        var xml2 = XDocument.Parse(await resp2.Content.ReadAsStringAsync());
        var names2 = xml2.Descendants("Name").Select(e => e.Value).ToList();
        Assert.That(names2.Count, Is.EqualTo(2));
        Assert.That(names1.Intersect(names2).Count(), Is.EqualTo(0));
    }

    // ── Table Entity Pagination ─────────────────────────────────────────

    [Test]
    public async Task QueryEntities_WithTop_ReturnsContinuationHeaders()
    {
        var tableName = "PaginateEntities";
        var body = JsonSerializer.Serialize(new { TableName = tableName });
        await TableHttp.PostAsync($"/{Account}/Tables",
            new StringContent(body, Encoding.UTF8, "application/json"));

        // Insert 5 entities
        for (int i = 1; i <= 5; i++)
        {
            var entity = JsonSerializer.Serialize(new { PartitionKey = "p", RowKey = $"r{i:D2}", Val = i });
            await TableHttp.PostAsync($"/{Account}/{tableName}",
                new StringContent(entity, Encoding.UTF8, "application/json"));
        }

        // First page of 2
        var resp1 = await TableHttp.GetAsync($"/{Account}/{tableName}()?$top=2");
        Assert.That(resp1.StatusCode, Is.EqualTo(HttpStatusCode.OK));

        var json1 = await resp1.Content.ReadAsStringAsync();
        using var doc1 = JsonDocument.Parse(json1);
        Assert.That(doc1.RootElement.GetProperty("value").GetArrayLength(), Is.EqualTo(2));

        // Should have continuation headers
        Assert.That(resp1.Headers.Contains("x-ms-continuation-NextPartitionKey"), Is.True);
        Assert.That(resp1.Headers.Contains("x-ms-continuation-NextRowKey"), Is.True);

        var nextPk = resp1.Headers.GetValues("x-ms-continuation-NextPartitionKey").First();
        var nextRk = resp1.Headers.GetValues("x-ms-continuation-NextRowKey").First();

        // Second page using continuation
        var resp2 = await TableHttp.GetAsync(
            $"/{Account}/{tableName}()?$top=2&NextPartitionKey={Uri.EscapeDataString(nextPk)}&NextRowKey={Uri.EscapeDataString(nextRk)}");
        var json2 = await resp2.Content.ReadAsStringAsync();
        using var doc2 = JsonDocument.Parse(json2);
        Assert.That(doc2.RootElement.GetProperty("value").GetArrayLength(), Is.EqualTo(2));
    }

    // ── No Pagination When All Fit ──────────────────────────────────────

    [Test]
    public async Task ListContainers_NoNextMarker_WhenAllFit()
    {
        await BlobHttp.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/nopage-c1?restype=container"));

        var resp = await BlobHttp.GetAsync($"/{Account}?comp=list&prefix=nopage-&maxresults=100");
        var xml = XDocument.Parse(await resp.Content.ReadAsStringAsync());
        var nextMarker = xml.Descendants("NextMarker").First().Value;
        Assert.That(nextMarker, Is.Empty, "Should have empty NextMarker when all results fit");
    }

    // ── Table Query No Continuation When All Fit ────────────────────────

    [Test]
    public async Task QueryEntities_NoContinuation_WhenAllFit()
    {
        var tableName = "NoPaginateEnt";
        var body = JsonSerializer.Serialize(new { TableName = tableName });
        await TableHttp.PostAsync($"/{Account}/Tables",
            new StringContent(body, Encoding.UTF8, "application/json"));

        await TableHttp.PostAsync($"/{Account}/{tableName}",
            new StringContent(JsonSerializer.Serialize(new { PartitionKey = "p", RowKey = "r1" }),
                Encoding.UTF8, "application/json"));

        var resp = await TableHttp.GetAsync($"/{Account}/{tableName}()?$top=100");
        Assert.That(resp.Headers.Contains("x-ms-continuation-NextPartitionKey"), Is.False);
    }
}
