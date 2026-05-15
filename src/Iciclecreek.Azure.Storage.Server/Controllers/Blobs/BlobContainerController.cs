using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Server.Infrastructure;
using Microsoft.AspNetCore.Mvc;

namespace Iciclecreek.Azure.Storage.Server.Controllers.Blobs;

/// <summary>
/// Container-level operations (create, delete, get properties, list blobs).
/// All require ?restype=container.
/// </summary>
[ApiController]
[ServicePortConstraint("Blob", 10000)]
[Route("{account}/{container}")]
public class BlobContainerController : ControllerBase
{
    private readonly BlobServiceClient _svc;
    public BlobContainerController(BlobServiceClient svc) => _svc = svc;

    /// <summary>PUT /{account}/{container}?restype=container — Create Container</summary>
    [HttpPut]
    [QueryConstraint("restype", "container")]
    [QueryAbsentConstraint("comp")]
    public async Task<IActionResult> CreateContainer(string account, string container)
    {
        var client = _svc.GetBlobContainerClient(container);
        await client.CreateIfNotExistsAsync();
        return StatusCode(201);
    }

    /// <summary>DELETE /{account}/{container}?restype=container — Delete Container</summary>
    [HttpDelete]
    [QueryConstraint("restype", "container")]
    public async Task<IActionResult> DeleteContainer(string account, string container)
    {
        var client = _svc.GetBlobContainerClient(container);
        await client.DeleteAsync();
        return StatusCode(202);
    }

    /// <summary>HEAD /{account}/{container}?restype=container — Get Container Properties</summary>
    [HttpHead]
    [QueryConstraint("restype", "container")]
    public async Task<IActionResult> GetContainerProperties(string account, string container)
    {
        var client = _svc.GetBlobContainerClient(container);
        var props = await client.GetPropertiesAsync();
        Response.Headers["ETag"] = props.Value.ETag.ToString();
        Response.Headers["Last-Modified"] = props.Value.LastModified.ToString("R");
        if (props.Value.Metadata != null)
        {
            foreach (var kv in props.Value.Metadata)
                Response.Headers[$"x-ms-meta-{kv.Key}"] = kv.Value;
        }
        return Ok();
    }

    /// <summary>GET /{account}/{container}?restype=container&amp;comp=list — List Blobs</summary>
    [HttpGet]
    [QueryConstraint("restype", "container")]
    [QueryConstraint("comp", "list")]
    public IActionResult ListBlobs(string account, string container,
        [FromQuery] string? prefix, [FromQuery] string? delimiter,
        [FromQuery] string? marker, [FromQuery] int? maxresults)
    {
        var client = _svc.GetBlobContainerClient(container);
        var endpoint = $"{Request.Scheme}://{Request.Host}/{account}";

        var allBlobs = new List<(string Name, long ContentLength, string? ContentType, string? ETag, DateTimeOffset LastModified)>();

        if (!string.IsNullOrEmpty(delimiter))
        {
            foreach (var item in client.GetBlobsByHierarchy(BlobTraits.None, BlobStates.None, delimiter, prefix))
            {
                if (item.IsBlob)
                {
                    allBlobs.Add((item.Blob.Name,
                        item.Blob.Properties.ContentLength ?? 0,
                        item.Blob.Properties.ContentType,
                        item.Blob.Properties.ETag?.ToString(),
                        item.Blob.Properties.LastModified ?? DateTimeOffset.UtcNow));
                }
            }
        }
        else
        {
            foreach (var b in client.GetBlobs(BlobTraits.None, BlobStates.None, prefix, default))
            {
                allBlobs.Add((b.Name,
                    b.Properties.ContentLength ?? 0,
                    b.Properties.ContentType,
                    b.Properties.ETag?.ToString(),
                    b.Properties.LastModified ?? DateTimeOffset.UtcNow));
            }
        }

        var (page, nextMarker) = PaginationHelper.Paginate(allBlobs, marker, maxresults, b => b.Name);
        return Content(XmlHelper.ListBlobsXml(endpoint, container, page, prefix, delimiter, nextMarker), "application/xml");
    }

    /// <summary>GET /{account}/{container}?restype=container (no comp) — Get Container Properties (GET variant)</summary>
    [HttpGet]
    [QueryConstraint("restype", "container")]
    [QueryAbsentConstraint("comp")]
    public async Task<IActionResult> GetContainerPropertiesGet(string account, string container)
    {
        var client = _svc.GetBlobContainerClient(container);
        var props = await client.GetPropertiesAsync();
        Response.Headers["ETag"] = props.Value.ETag.ToString();
        Response.Headers["Last-Modified"] = props.Value.LastModified.ToString("R");
        return Ok();
    }

    /// <summary>PUT /{account}/{container}?restype=container&amp;comp=metadata — Set Container Metadata</summary>
    [HttpPut]
    [QueryConstraint("restype", "container")]
    [QueryConstraint("comp", "metadata")]
    public async Task<IActionResult> SetContainerMetadata(string account, string container)
    {
        var client = _svc.GetBlobContainerClient(container);
        var metadata = new Dictionary<string, string>();
        foreach (var header in Request.Headers)
        {
            if (header.Key.StartsWith("x-ms-meta-", StringComparison.OrdinalIgnoreCase))
                metadata[header.Key.Substring("x-ms-meta-".Length)] = header.Value.ToString();
        }
        await client.SetMetadataAsync(metadata);
        return Ok();
    }

    /// <summary>PUT /{account}/{container}?restype=container&amp;comp=lease — Container Lease Operations</summary>
    [HttpPut]
    [QueryConstraint("restype", "container")]
    [QueryConstraint("comp", "lease")]
    public async Task<IActionResult> ContainerLease(string account, string container)
    {
        var client = _svc.GetBlobContainerClient(container);
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
                await leaseClient.BreakAsync();
                return Ok();

            default:
                return BadRequest("Missing or invalid x-ms-lease-action header.");
        }
    }
}
