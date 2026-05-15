using System.Text.Json;
using System.Text.RegularExpressions;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Iciclecreek.Azure.Storage.Server.Infrastructure;
using Microsoft.AspNetCore.Mvc;

namespace Iciclecreek.Azure.Storage.Server.Controllers.Tables;

/// <summary>
/// Entity-level operations: query, get, insert, update, merge, delete.
/// </summary>
[ApiController]
[ServicePortConstraint("Table", 10002)]
[Route("{account}/{table}")]
public class TableEntityController : ControllerBase
{
    private readonly TableServiceClient _svc;
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNamingPolicy = null };

    public TableEntityController(TableServiceClient svc) => _svc = svc;

    /// <summary>
    /// GET /{account}/{table}() — Query entities
    /// GET /{account}/{table}(PartitionKey='x',RowKey='y') — Get single entity
    /// The parenthesized key expression is captured via the raw URL.
    /// </summary>
    [HttpGet]
    public async Task<IActionResult> QueryOrGetEntities(string account, string table)
    {
        var rawPath = Request.Path.Value ?? "";
        var keys = ParseEntityKeys(rawPath);
        table = StripKeyExpression(table);
        var client = _svc.GetTableClient(table);
        var endpoint = $"{Request.Scheme}://{Request.Host}/{account}";

        // Parse query options
        var selectStr = Request.Query["$select"].FirstOrDefault();
        var selectFields = selectStr?.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var topStr = Request.Query["$top"].FirstOrDefault();
        int? top = int.TryParse(topStr, out var t) ? t : null;

        if (keys != null)
        {
            // Single entity get
            var entity = await client.GetEntityAsync<TableEntity>(keys.Value.pk, keys.Value.rk);
            var json = EntityToODataJson(entity.Value, endpoint, table, selectFields);
            return new ContentResult
            {
                Content = json,
                ContentType = "application/json;odata=minimalmetadata",
                StatusCode = 200
            };
        }
        else
        {
            // Query entities with pagination
            var filter = Request.Query["$filter"].FirstOrDefault();
            var nextPk = Request.Query["NextPartitionKey"].FirstOrDefault();
            var nextRk = Request.Query["NextRowKey"].FirstOrDefault();
            var pageSize = top ?? 1000;

            var entities = new List<string>();
            bool pastMarker = (nextPk == null && nextRk == null);
            TableEntity? lastEntity = null;
            bool hasMore = false;

            await foreach (var e in client.QueryAsync<TableEntity>(filter: filter))
            {
                // Skip past continuation token
                if (!pastMarker)
                {
                    if (string.Compare(e.PartitionKey, nextPk, StringComparison.Ordinal) > 0 ||
                        (e.PartitionKey == nextPk && string.Compare(e.RowKey, nextRk, StringComparison.Ordinal) > 0))
                        pastMarker = true;
                    else
                        continue;
                }

                if (entities.Count >= pageSize)
                {
                    hasMore = true;
                    break;
                }

                entities.Add(EntityToODataJson(e, endpoint, table, selectFields));
                lastEntity = e;
            }

            // Set continuation headers if there are more results
            if (hasMore && lastEntity != null)
            {
                Response.Headers["x-ms-continuation-NextPartitionKey"] = lastEntity.PartitionKey;
                Response.Headers["x-ms-continuation-NextRowKey"] = lastEntity.RowKey;
            }

            var result = $"{{\"odata.metadata\":\"{endpoint}/$metadata#{table}\",\"value\":[{string.Join(",", entities)}]}}";
            return new ContentResult
            {
                Content = result,
                ContentType = "application/json;odata=minimalmetadata",
                StatusCode = 200
            };
        }
    }

    /// <summary>POST /{account}/{table} — Insert Entity</summary>
    [HttpPost]
    public async Task<IActionResult> InsertEntity(string account, string table)
    {
        table = StripKeyExpression(table);
        var client = _svc.GetTableClient(table);
        var entity = await ParseEntityFromBodyAsync();
        var endpoint = $"{Request.Scheme}://{Request.Host}/{account}";

        await client.AddEntityAsync(entity);

        // Re-read to get server-assigned ETag/Timestamp
        var stored = await client.GetEntityAsync<TableEntity>(entity.PartitionKey, entity.RowKey);
        Response.Headers["ETag"] = stored.Value.ETag.ToString();
        var json = EntityToODataJson(stored.Value, endpoint, table);
        return new ContentResult
        {
            Content = json,
            ContentType = "application/json;odata=minimalmetadata",
            StatusCode = 201
        };
    }

    /// <summary>PUT /{account}/{table}(PartitionKey='x',RowKey='y') — Update Entity (Replace)</summary>
    [HttpPut]
    public async Task<IActionResult> UpdateEntity(string account, string table)
    {
        var rawPath = Request.Path.Value ?? "";
        var keys = ParseEntityKeys(rawPath);
        if (keys == null)
            return BadRequest("Missing entity keys in URL.");

        table = StripKeyExpression(table);
        var client = _svc.GetTableClient(table);
        var entity = await ParseEntityFromBodyAsync();
        entity.PartitionKey = keys.Value.pk;
        entity.RowKey = keys.Value.rk;

        var ifMatch = Request.Headers["If-Match"].FirstOrDefault();
        var etag = ifMatch != null ? new ETag(ifMatch) : ETag.All;

        await client.UpdateEntityAsync(entity, etag, TableUpdateMode.Replace);
        return StatusCode(204);
    }

    /// <summary>MERGE or PATCH /{account}/{table}(PartitionKey='x',RowKey='y') — Merge Entity</summary>
    [AcceptVerbs("MERGE", "PATCH")]
    public async Task<IActionResult> MergeEntity(string account, string table)
    {
        var rawPath = Request.Path.Value ?? "";
        var keys = ParseEntityKeys(rawPath);
        if (keys == null)
            return BadRequest("Missing entity keys in URL.");

        table = StripKeyExpression(table);
        var client = _svc.GetTableClient(table);
        var entity = await ParseEntityFromBodyAsync();
        entity.PartitionKey = keys.Value.pk;
        entity.RowKey = keys.Value.rk;

        var ifMatch = Request.Headers["If-Match"].FirstOrDefault();
        var etag = ifMatch != null ? new ETag(ifMatch) : ETag.All;

        await client.UpdateEntityAsync(entity, etag, TableUpdateMode.Merge);
        return StatusCode(204);
    }

    /// <summary>DELETE /{account}/{table}(PartitionKey='x',RowKey='y') — Delete Entity</summary>
    [HttpDelete]
    public async Task<IActionResult> DeleteEntity(string account, string table)
    {
        var rawPath = Request.Path.Value ?? "";
        var keys = ParseEntityKeys(rawPath);
        if (keys == null)
            return BadRequest("Missing entity keys in URL.");

        table = StripKeyExpression(table);
        var client = _svc.GetTableClient(table);
        var ifMatch = Request.Headers["If-Match"].FirstOrDefault();
        var etag = ifMatch != null ? new ETag(ifMatch) : ETag.All;

        await client.DeleteEntityAsync(keys.Value.pk, keys.Value.rk, etag);
        return StatusCode(204);
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    /// <summary>
    /// Strips the trailing key expression from the table route parameter.
    /// e.g. "mytable(PartitionKey='a',RowKey='b')" → "mytable", "mytable()" → "mytable", "mytable" → "mytable"
    /// </summary>
    private static string StripKeyExpression(string table)
    {
        var idx = table.IndexOf('(');
        return idx >= 0 ? table[..idx] : table;
    }

    private static readonly Regex KeysRegex = new(
        @"\(PartitionKey='(?<pk>[^']*)',\s*RowKey='(?<rk>[^']*)'\)\s*$",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static (string pk, string rk)? ParseEntityKeys(string path)
    {
        var match = KeysRegex.Match(path);
        if (!match.Success)
            return null;
        return (
            Uri.UnescapeDataString(match.Groups["pk"].Value),
            Uri.UnescapeDataString(match.Groups["rk"].Value));
    }

    private async Task<TableEntity> ParseEntityFromBodyAsync()
    {
        using var doc = await JsonDocument.ParseAsync(Request.Body);
        var entity = new TableEntity();

        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            var name = prop.Name;
            // Skip OData metadata properties
            if (name.StartsWith("odata.", StringComparison.OrdinalIgnoreCase))
                continue;

            switch (name)
            {
                case "PartitionKey":
                    entity.PartitionKey = prop.Value.GetString()!;
                    break;
                case "RowKey":
                    entity.RowKey = prop.Value.GetString()!;
                    break;
                case "Timestamp":
                    // Server-managed, ignore from input
                    break;
                default:
                    entity[name] = DeserializeValue(prop, doc.RootElement);
                    break;
            }
        }

        return entity;
    }

    private static object? DeserializeValue(JsonProperty prop, JsonElement root)
    {
        // Check for OData type annotation: PropertyName@odata.type
        var typeProp = $"{prop.Name}@odata.type";
        string? odataType = null;
        if (root.TryGetProperty(typeProp, out var typeElement))
            odataType = typeElement.GetString();

        return prop.Value.ValueKind switch
        {
            JsonValueKind.String when odataType == "Edm.DateTime"
                => DateTimeOffset.Parse(prop.Value.GetString()!),
            JsonValueKind.String when odataType == "Edm.Guid"
                => Guid.Parse(prop.Value.GetString()!),
            JsonValueKind.String when odataType == "Edm.Binary"
                => Convert.FromBase64String(prop.Value.GetString()!),
            JsonValueKind.String when odataType == "Edm.Int64"
                => long.Parse(prop.Value.GetString()!),
            JsonValueKind.String => prop.Value.GetString(),
            JsonValueKind.Number when prop.Value.TryGetInt32(out var i) => i,
            JsonValueKind.Number when prop.Value.TryGetInt64(out var l) => l,
            JsonValueKind.Number => prop.Value.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => prop.Value.GetRawText()
        };
    }

    private static string EntityToODataJson(TableEntity entity, string endpoint, string table, HashSet<string>? select = null)
    {
        var dict = new Dictionary<string, object?>();
        dict["odata.etag"] = entity.ETag.ToString();
        // PartitionKey, RowKey, Timestamp are always included
        dict["PartitionKey"] = entity.PartitionKey;
        dict["RowKey"] = entity.RowKey;
        dict["Timestamp"] = entity.Timestamp?.ToString("O");

        foreach (var key in entity.Keys)
        {
            if (key is "odata.etag" or "PartitionKey" or "RowKey" or "Timestamp")
                continue;

            // If $select is specified, only include selected properties
            if (select != null && !select.Contains(key))
                continue;

            var val = entity[key];
            switch (val)
            {
                case DateTimeOffset dto:
                    dict[$"{key}@odata.type"] = "Edm.DateTime";
                    dict[key] = dto.UtcDateTime.ToString("O");
                    break;
                case DateTime dt:
                    dict[$"{key}@odata.type"] = "Edm.DateTime";
                    dict[key] = dt.ToUniversalTime().ToString("O");
                    break;
                case Guid g:
                    dict[$"{key}@odata.type"] = "Edm.Guid";
                    dict[key] = g.ToString();
                    break;
                case long l:
                    dict[$"{key}@odata.type"] = "Edm.Int64";
                    dict[key] = l.ToString();
                    break;
                case byte[] bytes:
                    dict[$"{key}@odata.type"] = "Edm.Binary";
                    dict[key] = Convert.ToBase64String(bytes);
                    break;
                case BinaryData bd:
                    dict[$"{key}@odata.type"] = "Edm.Binary";
                    dict[key] = Convert.ToBase64String(bd.ToArray());
                    break;
                default:
                    dict[key] = val;
                    break;
            }
        }

        return JsonSerializer.Serialize(dict, JsonOptions);
    }
}
