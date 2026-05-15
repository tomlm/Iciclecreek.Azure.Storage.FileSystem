using System.Xml.Linq;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Server.Infrastructure;
using Microsoft.AspNetCore.Mvc;

namespace Iciclecreek.Azure.Storage.Server.Controllers.Blobs;

/// <summary>
/// Blob-level operations (upload, download, delete, properties, block operations).
/// Route uses {**blob} catch-all for hierarchical blob names.
/// </summary>
[ApiController]
[ServicePortConstraint("Blob", 10000)]
[Route("{account}/{container}/{**blob}")]
public class BlobController : ControllerBase
{
    private readonly BlobServiceClient _svc;
    public BlobController(BlobServiceClient svc) => _svc = svc;

    // ── Put Blob ────────────────────────────────────────────────────────

    /// <summary>PUT /{account}/{container}/{blob} — Upload Blob</summary>
    [HttpPut]
    [QueryAbsentConstraint("comp")]
    public async Task<IActionResult> PutBlob(string account, string container, string blob)
    {
        var blobType = Request.Headers["x-ms-blob-type"].FirstOrDefault();

        // AppendBlob creation (empty blob)
        if (string.Equals(blobType, "AppendBlob", StringComparison.OrdinalIgnoreCase))
        {
            var appendClient = _svc.GetBlobContainerClient(container).GetAppendBlobClient(blob);
            var result = await appendClient.CreateAsync();
            Response.Headers["ETag"] = result.Value.ETag.ToString();
            Response.Headers["Last-Modified"] = result.Value.LastModified.ToString("R");
            return StatusCode(201);
        }

        // PageBlob creation (pre-allocated)
        if (string.Equals(blobType, "PageBlob", StringComparison.OrdinalIgnoreCase))
        {
            var pageClient = GetPageBlobClient(container, blob);
            var sizeStr = Request.Headers["x-ms-blob-content-length"].FirstOrDefault();
            var size = long.TryParse(sizeStr, out var s) ? s : 0;
            var result = await pageClient.CreateAsync(size, (PageBlobCreateOptions?)null);
            Response.Headers["ETag"] = result.Value.ETag.ToString();
            Response.Headers["Last-Modified"] = result.Value.LastModified.ToString("R");
            return StatusCode(201);
        }

        // Default: Block blob upload
        var client = _svc.GetBlobContainerClient(container).GetBlobClient(blob);

        var options = new BlobUploadOptions();
        var headers = new BlobHttpHeaders();

        if (Request.Headers.TryGetValue("x-ms-blob-content-type", out var ct))
            headers.ContentType = ct;
        else if (Request.ContentType != null)
            headers.ContentType = Request.ContentType;

        if (Request.Headers.TryGetValue("x-ms-blob-content-encoding", out var ce))
            headers.ContentEncoding = ce;
        if (Request.Headers.TryGetValue("x-ms-blob-content-language", out var cl))
            headers.ContentLanguage = cl;
        if (Request.Headers.TryGetValue("x-ms-blob-content-disposition", out var cd))
            headers.ContentDisposition = cd;
        if (Request.Headers.TryGetValue("x-ms-blob-cache-control", out var cc))
            headers.CacheControl = cc;

        options.HttpHeaders = headers;

        // Metadata from x-ms-meta-* headers
        var metadata = ExtractMetadata();
        if (metadata.Count > 0)
            options.Metadata = metadata;

        // ETag conditions
        var conditions = ExtractConditions();
        if (conditions != null)
            options.Conditions = conditions;

        var info = await client.UploadAsync(Request.Body, options);

        Response.Headers["ETag"] = info.Value.ETag.ToString();
        Response.Headers["Last-Modified"] = info.Value.LastModified.ToString("R");
        Response.Headers["Content-MD5"] = info.Value.ContentHash != null
            ? Convert.ToBase64String(info.Value.ContentHash) : "";

        return StatusCode(201);
    }

    // ── Get Blob (download) ─────────────────────────────────────────────

    /// <summary>GET /{account}/{container}/{blob} — Download Blob</summary>
    [HttpGet]
    [QueryAbsentConstraint("comp")]
    [QueryAbsentConstraint("restype")]
    public async Task<IActionResult> DownloadBlob(string account, string container, string blob)
    {
        var client = _svc.GetBlobContainerClient(container).GetBlobClient(blob);
        var download = await client.DownloadContentAsync();
        var props = download.Value.Details;

        Response.Headers["ETag"] = props.ETag.ToString();
        Response.Headers["Last-Modified"] = props.LastModified.ToString("R");
        if (props.ContentHash != null)
            Response.Headers["Content-MD5"] = Convert.ToBase64String(props.ContentHash);
        foreach (var kv in props.Metadata)
            Response.Headers[$"x-ms-meta-{kv.Key}"] = kv.Value;

        return File(download.Value.Content.ToArray(),
            props.ContentType ?? "application/octet-stream");
    }

    // ── Head Blob ───────────────────────────────────────────────────────

    /// <summary>HEAD /{account}/{container}/{blob} — Get Blob Properties</summary>
    [HttpHead]
    [QueryAbsentConstraint("comp")]
    public async Task<IActionResult> GetBlobProperties(string account, string container, string blob)
    {
        var client = _svc.GetBlobContainerClient(container).GetBlobClient(blob);
        var props = await client.GetPropertiesAsync();

        Response.Headers["ETag"] = props.Value.ETag.ToString();
        Response.Headers["Last-Modified"] = props.Value.LastModified.ToString("R");
        Response.Headers["Content-Length"] = props.Value.ContentLength.ToString();
        Response.Headers["Content-Type"] = props.Value.ContentType ?? "application/octet-stream";
        if (!string.IsNullOrEmpty(props.Value.ContentEncoding))
            Response.Headers["Content-Encoding"] = props.Value.ContentEncoding;
        if (props.Value.ContentHash != null)
            Response.Headers["Content-MD5"] = Convert.ToBase64String(props.Value.ContentHash);
        Response.Headers["x-ms-blob-type"] = props.Value.BlobType.ToString();

        foreach (var kv in props.Value.Metadata)
            Response.Headers[$"x-ms-meta-{kv.Key}"] = kv.Value;

        return Ok();
    }

    // ── Delete Blob ─────────────────────────────────────────────────────

    /// <summary>DELETE /{account}/{container}/{blob} — Delete Blob</summary>
    [HttpDelete]
    [QueryAbsentConstraint("restype")]
    public async Task<IActionResult> DeleteBlob(string account, string container, string blob)
    {
        var client = _svc.GetBlobContainerClient(container).GetBlobClient(blob);
        await client.DeleteIfExistsAsync();
        return StatusCode(202);
    }

    // ── Stage Block ─────────────────────────────────────────────────────

    /// <summary>PUT /{account}/{container}/{blob}?comp=block&amp;blockid=xxx — Stage Block</summary>
    [HttpPut]
    [QueryConstraint("comp", "block")]
    public async Task<IActionResult> StageBlock(string account, string container, string blob, [FromQuery] string blockid)
    {
        var client = _svc.GetBlobContainerClient(container).GetBlockBlobClient(blob);
        await client.StageBlockAsync(blockid, Request.Body);
        return StatusCode(201);
    }

    // ── Commit Block List ───────────────────────────────────────────────

    /// <summary>PUT /{account}/{container}/{blob}?comp=blocklist — Commit Block List</summary>
    [HttpPut]
    [QueryConstraint("comp", "blocklist")]
    public async Task<IActionResult> CommitBlockList(string account, string container, string blob)
    {
        var client = _svc.GetBlobContainerClient(container).GetBlockBlobClient(blob);

        // Parse the XML request body: <BlockList><Latest>blockid</Latest>...</BlockList>
        var doc = await XDocument.LoadAsync(Request.Body, LoadOptions.None, HttpContext.RequestAborted);
        var blockIds = doc.Root!.Elements()
            .Select(e => e.Value)
            .ToList();

        var options = new CommitBlockListOptions();

        if (Request.Headers.TryGetValue("x-ms-blob-content-type", out var ct))
        {
            options.HttpHeaders ??= new BlobHttpHeaders();
            options.HttpHeaders.ContentType = ct;
        }

        var info = await client.CommitBlockListAsync(blockIds, options);

        Response.Headers["ETag"] = info.Value.ETag.ToString();
        Response.Headers["Last-Modified"] = info.Value.LastModified.ToString("R");
        return StatusCode(201);
    }

    // ── Get Block List ──────────────────────────────────────────────────

    /// <summary>GET /{account}/{container}/{blob}?comp=blocklist — Get Block List</summary>
    [HttpGet]
    [QueryConstraint("comp", "blocklist")]
    public async Task<IActionResult> GetBlockList(string account, string container, string blob)
    {
        var client = _svc.GetBlobContainerClient(container).GetBlockBlobClient(blob);
        var blockList = await client.GetBlockListAsync();

        var committed = blockList.Value.CommittedBlocks
            .Select(b => (b.Name, b.SizeLong));
        var uncommitted = blockList.Value.UncommittedBlocks
            .Select(b => (b.Name, b.SizeLong));

        return Content(XmlHelper.BlockListXml(committed, uncommitted), "application/xml");
    }

    // ── Set Blob Properties ─────────────────────────────────────────────

    /// <summary>PUT /{account}/{container}/{blob}?comp=properties — Set Blob HTTP Headers</summary>
    [HttpPut]
    [QueryConstraint("comp", "properties")]
    public async Task<IActionResult> SetBlobProperties(string account, string container, string blob)
    {
        var client = _svc.GetBlobContainerClient(container).GetBlobClient(blob);
        var headers = new BlobHttpHeaders();

        if (Request.Headers.TryGetValue("x-ms-blob-content-type", out var ct))
            headers.ContentType = ct;
        if (Request.Headers.TryGetValue("x-ms-blob-content-encoding", out var ce))
            headers.ContentEncoding = ce;
        if (Request.Headers.TryGetValue("x-ms-blob-content-language", out var cl))
            headers.ContentLanguage = cl;
        if (Request.Headers.TryGetValue("x-ms-blob-content-disposition", out var cd))
            headers.ContentDisposition = cd;
        if (Request.Headers.TryGetValue("x-ms-blob-cache-control", out var cc))
            headers.CacheControl = cc;

        await client.SetHttpHeadersAsync(headers);
        return Ok();
    }

    /// <summary>PUT /{account}/{container}/{blob}?comp=metadata — Set Blob Metadata</summary>
    [HttpPut]
    [QueryConstraint("comp", "metadata")]
    public async Task<IActionResult> SetBlobMetadata(string account, string container, string blob)
    {
        var client = _svc.GetBlobContainerClient(container).GetBlobClient(blob);
        var metadata = ExtractMetadata();
        await client.SetMetadataAsync(metadata);
        return Ok();
    }

    // ── Tags ────────────────────────────────────────────────────────────

    /// <summary>GET /{account}/{container}/{blob}?comp=tags — Get Blob Tags</summary>
    [HttpGet]
    [QueryConstraint("comp", "tags")]
    public async Task<IActionResult> GetBlobTags(string account, string container, string blob)
    {
        var client = _svc.GetBlobContainerClient(container).GetBlobClient(blob);
        var result = await client.GetTagsAsync();

        var xml = new System.Xml.Linq.XElement("Tags",
            new System.Xml.Linq.XElement("TagSet",
                result.Value.Tags.Select(t =>
                    new System.Xml.Linq.XElement("Tag",
                        new System.Xml.Linq.XElement("Key", t.Key),
                        new System.Xml.Linq.XElement("Value", t.Value)))));
        return Content($"<?xml version=\"1.0\" encoding=\"utf-8\"?>{xml}", "application/xml");
    }

    /// <summary>PUT /{account}/{container}/{blob}?comp=tags — Set Blob Tags</summary>
    [HttpPut]
    [QueryConstraint("comp", "tags")]
    public async Task<IActionResult> SetBlobTags(string account, string container, string blob)
    {
        var client = _svc.GetBlobContainerClient(container).GetBlobClient(blob);
        var doc = await System.Xml.Linq.XDocument.LoadAsync(Request.Body, System.Xml.Linq.LoadOptions.None, HttpContext.RequestAborted);
        var tags = new Dictionary<string, string>();
        foreach (var tag in doc.Descendants("Tag"))
        {
            var key = tag.Element("Key")?.Value;
            var value = tag.Element("Value")?.Value;
            if (key != null) tags[key] = value ?? "";
        }
        await client.SetTagsAsync(tags);
        return StatusCode(204);
    }

    // ── Snapshot ────────────────────────────────────────────────────────

    /// <summary>PUT /{account}/{container}/{blob}?comp=snapshot — Create Snapshot</summary>
    [HttpPut]
    [QueryConstraint("comp", "snapshot")]
    public async Task<IActionResult> CreateSnapshot(string account, string container, string blob)
    {
        var client = _svc.GetBlobContainerClient(container).GetBlobClient(blob);
        var result = await client.CreateSnapshotAsync();
        Response.Headers["ETag"] = result.Value.ETag.ToString();
        Response.Headers["x-ms-snapshot"] = result.Value.Snapshot;
        return StatusCode(201);
    }

    // ── Lease ───────────────────────────────────────────────────────────

    /// <summary>PUT /{account}/{container}/{blob}?comp=lease — Blob Lease Operations</summary>
    [HttpPut]
    [QueryConstraint("comp", "lease")]
    public async Task<IActionResult> BlobLease(string account, string container, string blob)
    {
        var client = _svc.GetBlobContainerClient(container).GetBlobClient(blob);
        var action = Request.Headers["x-ms-lease-action"].FirstOrDefault()?.ToLowerInvariant();

        var leaseId = Request.Headers["x-ms-lease-id"].FirstOrDefault();
        var leaseClient = client.GetBlobLeaseClient(leaseId);

        switch (action)
        {
            case "acquire":
                var durationStr = Request.Headers["x-ms-lease-duration"].FirstOrDefault();
                var duration = int.TryParse(durationStr, out var d) ? d : -1;
                var acquired = await leaseClient.AcquireAsync(TimeSpan.FromSeconds(duration));
                Response.Headers["x-ms-lease-id"] = acquired.Value.LeaseId;
                return StatusCode(201);

            case "renew":
                var renewed = await leaseClient.RenewAsync();
                Response.Headers["x-ms-lease-id"] = renewed.Value.LeaseId;
                return Ok();

            case "release":
                await leaseClient.ReleaseAsync();
                return Ok();

            case "change":
                var proposedId = Request.Headers["x-ms-proposed-lease-id"].FirstOrDefault() ?? Guid.NewGuid().ToString();
                var changed = await leaseClient.ChangeAsync(proposedId);
                Response.Headers["x-ms-lease-id"] = changed.Value.LeaseId;
                return Ok();

            case "break":
                var breakResult = await leaseClient.BreakAsync();
                return Ok();

            default:
                return BadRequest("Missing or invalid x-ms-lease-action header.");
        }
    }

    // ── AppendBlob ──────────────────────────────────────────────────────

    /// <summary>PUT /{account}/{container}/{blob}?comp=appendblock — Append Block</summary>
    [HttpPut]
    [QueryConstraint("comp", "appendblock")]
    public async Task<IActionResult> AppendBlock(string account, string container, string blob)
    {
        var client = _svc.GetBlobContainerClient(container).GetAppendBlobClient(blob);
        var result = await client.AppendBlockAsync(Request.Body);
        Response.Headers["ETag"] = result.Value.ETag.ToString();
        Response.Headers["x-ms-blob-append-offset"] = result.Value.BlobAppendOffset;
        Response.Headers["x-ms-blob-committed-block-count"] = result.Value.BlobCommittedBlockCount.ToString();
        return StatusCode(201);
    }

    // ── Page Blob ───────────────────────────────────────────────────────

    /// <summary>PUT /{account}/{container}/{blob}?comp=page — Upload/Clear Pages</summary>
    [HttpPut]
    [QueryConstraint("comp", "page")]
    public async Task<IActionResult> PageOperation(string account, string container, string blob)
    {
        var pageWrite = Request.Headers["x-ms-page-write"].FirstOrDefault()?.ToLowerInvariant();
        var client = GetPageBlobClient(container, blob);

        if (pageWrite == "update")
        {
            var rangeHeader = Request.Headers["x-ms-range"].FirstOrDefault()
                ?? Request.Headers["Range"].FirstOrDefault();
            var offset = ParseRangeOffset(rangeHeader);

            using var ms = new MemoryStream();
            await Request.Body.CopyToAsync(ms, HttpContext.RequestAborted);
            ms.Position = 0;

            var result = await client.UploadPagesAsync(ms, offset, (PageBlobUploadPagesOptions?)null);
            Response.Headers["ETag"] = result.Value.ETag.ToString();
            return StatusCode(201);
        }
        else if (pageWrite == "clear")
        {
            var rangeHeader = Request.Headers["x-ms-range"].FirstOrDefault()
                ?? Request.Headers["Range"].FirstOrDefault();
            var range = ParseHttpRange(rangeHeader);
            var result = await client.ClearPagesAsync(range);
            Response.Headers["ETag"] = result.Value.ETag.ToString();
            return StatusCode(201);
        }

        return BadRequest("Missing or invalid x-ms-page-write header.");
    }

    /// <summary>GET /{account}/{container}/{blob}?comp=pagelist — Get Page Ranges</summary>
    [HttpGet]
    [QueryConstraint("comp", "pagelist")]
    public async Task<IActionResult> GetPageRanges(string account, string container, string blob)
    {
        var client = GetPageBlobClient(container, blob);
        var result = await client.GetPageRangesAsync();

        var xml = new System.Xml.Linq.XElement("PageList",
            result.Value.PageRanges.Select(r =>
                new System.Xml.Linq.XElement("PageRange",
                    new System.Xml.Linq.XElement("Start", r.Offset),
                    new System.Xml.Linq.XElement("End", r.Offset + (r.Length ?? 0) - 1))));
        return Content($"<?xml version=\"1.0\" encoding=\"utf-8\"?>{xml}", "application/xml");
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    /// <summary>
    /// Gets a PageBlobClient, using the Core method if available on the container client type.
    /// Extension method GetPageBlobClient may not delegate to GetPageBlobClientCore in all SDK versions.
    /// </summary>
    private PageBlobClient GetPageBlobClient(string container, string blob)
    {
        var containerClient = _svc.GetBlobContainerClient(container);
        // Try public GetFilePageBlobClient if available (providers that shadow GetPageBlobClient)
        var method = containerClient.GetType().GetMethod("GetFilePageBlobClient",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public,
            null, new[] { typeof(string) }, null);
        if (method != null)
            return (PageBlobClient)method.Invoke(containerClient, new object[] { blob })!;
        return containerClient.GetPageBlobClient(blob);
    }

    private static long ParseRangeOffset(string? rangeHeader)
    {
        // Parse "bytes=offset-end" format
        if (rangeHeader != null && rangeHeader.StartsWith("bytes=", StringComparison.OrdinalIgnoreCase))
        {
            var parts = rangeHeader[6..].Split('-');
            if (long.TryParse(parts[0], out var offset)) return offset;
        }
        return 0;
    }

    private static HttpRange ParseHttpRange(string? rangeHeader)
    {
        if (rangeHeader != null && rangeHeader.StartsWith("bytes=", StringComparison.OrdinalIgnoreCase))
        {
            var parts = rangeHeader[6..].Split('-');
            if (parts.Length == 2 && long.TryParse(parts[0], out var start) && long.TryParse(parts[1], out var end))
                return new HttpRange(start, end - start + 1);
        }
        return default;
    }

    private Dictionary<string, string> ExtractMetadata()
    {
        var metadata = new Dictionary<string, string>();
        foreach (var header in Request.Headers)
        {
            if (header.Key.StartsWith("x-ms-meta-", StringComparison.OrdinalIgnoreCase))
                metadata[header.Key.Substring("x-ms-meta-".Length)] = header.Value.ToString();
        }
        return metadata;
    }

    private BlobRequestConditions? ExtractConditions()
    {
        var ifMatch = Request.Headers["If-Match"].FirstOrDefault();
        var ifNoneMatch = Request.Headers["If-None-Match"].FirstOrDefault();

        if (ifMatch == null && ifNoneMatch == null)
            return null;

        var conditions = new BlobRequestConditions();
        if (ifMatch != null)
            conditions.IfMatch = new ETag(ifMatch);
        if (ifNoneMatch != null)
            conditions.IfNoneMatch = new ETag(ifNoneMatch);
        return conditions;
    }
}
