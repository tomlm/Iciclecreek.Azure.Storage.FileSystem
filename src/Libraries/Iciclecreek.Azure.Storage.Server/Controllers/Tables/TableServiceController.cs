using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Iciclecreek.Azure.Storage.Server.Infrastructure;
using Microsoft.AspNetCore.Mvc;

namespace Iciclecreek.Azure.Storage.Server.Controllers.Tables;

/// <summary>
/// Table-level operations: query tables, create table, delete table.
/// </summary>
[ApiController]
[ServicePortConstraint("Table", 10002)]
[Route("{account}")]
public class TableServiceController : ControllerBase
{
    private readonly TableServiceClient _svc;
    public TableServiceController(TableServiceClient svc) => _svc = svc;

    /// <summary>GET /{account}/Tables — Query Tables</summary>
    [HttpGet("Tables")]
    public IActionResult QueryTables(string account, [FromQuery(Name = "$top")] int? top,
        [FromQuery] string? NextTableName)
    {
        var endpoint = $"{Request.Scheme}://{Request.Host}/{account}";
        var allTables = new List<string>();

        foreach (var t in _svc.Query())
            allTables.Add(t.Name);

        var (page, nextMarker) = PaginationHelper.Paginate(allTables, NextTableName, top, n => n);

        if (nextMarker != null)
            Response.Headers["x-ms-continuation-NextTableName"] = nextMarker;

        return new JsonResult(new
        {
            @odata_metadata = $"{endpoint}/$metadata#Tables",
            value = page.Select(n => new { TableName = n })
        }, new JsonSerializerOptions { PropertyNamingPolicy = null })
        {
            ContentType = "application/json;odata=minimalmetadata",
            StatusCode = 200
        };
    }

    /// <summary>POST /{account}/Tables — Create Table</summary>
    [HttpPost("Tables")]
    public async Task<IActionResult> CreateTable(string account)
    {
        using var doc = await JsonDocument.ParseAsync(Request.Body);
        var tableName = doc.RootElement.GetProperty("TableName").GetString()!;

        await _svc.CreateTableIfNotExistsAsync(tableName);

        var endpoint = $"{Request.Scheme}://{Request.Host}/{account}";
        return new JsonResult(new
        {
            @odata_metadata = $"{endpoint}/$metadata#Tables/@Element",
            TableName = tableName
        }, new JsonSerializerOptions { PropertyNamingPolicy = null })
        {
            ContentType = "application/json;odata=minimalmetadata",
            StatusCode = 201
        };
    }

    /// <summary>DELETE /{account}/Tables('{tableName}') — Delete Table</summary>
    [HttpDelete("Tables('{tableName}')")]
    public async Task<IActionResult> DeleteTable(string account, string tableName)
    {
        await _svc.DeleteTableAsync(tableName);
        return StatusCode(204);
    }

    /// <summary>POST /{account}/$batch — Batch Transaction</summary>
    [HttpPost("$batch")]
    public async Task<IActionResult> Batch(string account)
    {
        var contentType = Request.ContentType ?? "";
        var boundaryMatch = Regex.Match(contentType, @"boundary=(\S+)");
        if (!boundaryMatch.Success)
            return BadRequest("Missing boundary in Content-Type.");

        var boundary = boundaryMatch.Groups[1].Value;
        var body = await new StreamReader(Request.Body).ReadToEndAsync();
        var parts = ParseMultipartBatch(body, boundary);

        if (parts.Count == 0)
            return BadRequest("No operations found in batch request.");

        // All operations must target the same table
        var tableName = parts[0].TableName;
        var client = _svc.GetTableClient(tableName);

        // Build transaction actions
        var actions = new List<TableTransactionAction>();
        foreach (var part in parts)
        {
            var entity = new TableEntity();
            if (part.Body != null)
            {
                using var doc = JsonDocument.Parse(part.Body);
                foreach (var prop in doc.RootElement.EnumerateObject())
                {
                    if (prop.Name.StartsWith("odata.", StringComparison.OrdinalIgnoreCase)) continue;
                    switch (prop.Name)
                    {
                        case "PartitionKey": entity.PartitionKey = prop.Value.GetString()!; break;
                        case "RowKey": entity.RowKey = prop.Value.GetString()!; break;
                        case "Timestamp": break;
                        default:
                            entity[prop.Name] = prop.Value.ValueKind switch
                            {
                                JsonValueKind.String => prop.Value.GetString(),
                                JsonValueKind.Number when prop.Value.TryGetInt32(out var i) => i,
                                JsonValueKind.Number when prop.Value.TryGetInt64(out var l) => l,
                                JsonValueKind.Number => prop.Value.GetDouble(),
                                JsonValueKind.True => true,
                                JsonValueKind.False => false,
                                _ => prop.Value.GetRawText()
                            };
                            break;
                    }
                }
            }

            // Extract keys from URL for non-POST operations
            if (part.PartitionKey != null) entity.PartitionKey = part.PartitionKey;
            if (part.RowKey != null) entity.RowKey = part.RowKey;

            var etag = part.IfMatch != null ? new ETag(part.IfMatch) : ETag.All;

            var actionType = part.Method.ToUpperInvariant() switch
            {
                "POST" => TableTransactionActionType.Add,
                "PUT" => TableTransactionActionType.UpdateReplace,
                "MERGE" or "PATCH" => TableTransactionActionType.UpdateMerge,
                "DELETE" => TableTransactionActionType.Delete,
                _ => throw new RequestFailedException(400, $"Unsupported method: {part.Method}", "BadRequest", null)
            };

            actions.Add(new TableTransactionAction(actionType, entity, etag));
        }

        var results = await client.SubmitTransactionAsync(actions);

        // Build multipart response
        var responseBoundary = $"batchresponse_{Guid.NewGuid()}";
        var sb = new StringBuilder();
        for (int i = 0; i < results.Value.Count; i++)
        {
            var r = results.Value[i];
            sb.AppendLine($"--{responseBoundary}");
            sb.AppendLine("Content-Type: application/http");
            sb.AppendLine("Content-Transfer-Encoding: binary");
            sb.AppendLine();
            var status = actions[i].ActionType == TableTransactionActionType.Add ? 201 : 204;
            sb.AppendLine($"HTTP/1.1 {status} {(status == 201 ? "Created" : "No Content")}");
            sb.AppendLine($"Content-ID: {i + 1}");
            sb.AppendLine();
        }
        sb.AppendLine($"--{responseBoundary}--");

        return new ContentResult
        {
            Content = sb.ToString(),
            ContentType = $"multipart/mixed;boundary={responseBoundary}",
            StatusCode = 202
        };
    }

    // ── Batch parsing helpers ───────────────────────────────────────────

    private static readonly Regex UrlKeysRegex = new(
        @"\(PartitionKey='(?<pk>[^']*)',\s*RowKey='(?<rk>[^']*)'\)",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private sealed class BatchPart
    {
        public string Method { get; set; } = "";
        public string TableName { get; set; } = "";
        public string? PartitionKey { get; set; }
        public string? RowKey { get; set; }
        public string? IfMatch { get; set; }
        public string? Body { get; set; }
    }

    private static List<BatchPart> ParseMultipartBatch(string body, string boundary)
    {
        var parts = new List<BatchPart>();
        var sections = body.Split(new[] { $"--{boundary}" }, StringSplitOptions.RemoveEmptyEntries);

        foreach (var section in sections)
        {
            if (section.TrimStart().StartsWith("--")) continue; // closing boundary
            if (!section.Contains("HTTP/")) continue;

            var part = new BatchPart();

            // Find the inner HTTP request line
            var lines = section.Split('\n').Select(l => l.TrimEnd('\r')).ToArray();
            var httpLineIdx = -1;
            for (int i = 0; i < lines.Length; i++)
            {
                if (lines[i].StartsWith("POST ", StringComparison.OrdinalIgnoreCase) ||
                    lines[i].StartsWith("PUT ", StringComparison.OrdinalIgnoreCase) ||
                    lines[i].StartsWith("MERGE ", StringComparison.OrdinalIgnoreCase) ||
                    lines[i].StartsWith("PATCH ", StringComparison.OrdinalIgnoreCase) ||
                    lines[i].StartsWith("DELETE ", StringComparison.OrdinalIgnoreCase))
                {
                    httpLineIdx = i;
                    break;
                }
            }

            if (httpLineIdx < 0) continue;

            var httpLine = lines[httpLineIdx];
            var httpParts = httpLine.Split(' ');
            part.Method = httpParts[0];
            var url = httpParts.Length > 1 ? httpParts[1] : "";

            // Extract table name from URL: /account/tableName or /account/tableName(...)
            var urlSegments = url.Split('/', StringSplitOptions.RemoveEmptyEntries);
            if (urlSegments.Length >= 2)
            {
                var tableSegment = urlSegments[^1]; // last segment
                var parenIdx = tableSegment.IndexOf('(');
                part.TableName = parenIdx >= 0 ? tableSegment[..parenIdx] : tableSegment;

                // Extract keys from URL
                var keyMatch = UrlKeysRegex.Match(tableSegment);
                if (keyMatch.Success)
                {
                    part.PartitionKey = Uri.UnescapeDataString(keyMatch.Groups["pk"].Value);
                    part.RowKey = Uri.UnescapeDataString(keyMatch.Groups["rk"].Value);
                }
            }

            // Parse headers after HTTP line
            for (int i = httpLineIdx + 1; i < lines.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(lines[i]))
                {
                    // Body starts after blank line
                    var bodyLines = lines.Skip(i + 1).Where(l => !string.IsNullOrWhiteSpace(l));
                    var bodyStr = string.Join("\n", bodyLines).Trim();
                    if (!string.IsNullOrEmpty(bodyStr) && bodyStr.StartsWith("{"))
                        part.Body = bodyStr;
                    break;
                }

                if (lines[i].StartsWith("If-Match:", StringComparison.OrdinalIgnoreCase))
                    part.IfMatch = lines[i]["If-Match:".Length..].Trim();
            }

            parts.Add(part);
        }

        return parts;
    }
}
