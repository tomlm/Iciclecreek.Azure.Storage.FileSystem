using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.Server.Infrastructure;
using Microsoft.AspNetCore.Mvc;

namespace Iciclecreek.Azure.Storage.Server.Controllers.Blobs;

/// <summary>
/// Account-level blob service operations (list containers).
/// </summary>
[ApiController]
[ServicePortConstraint("Blob", 10000)]
[Route("{account}")]
public class BlobServiceController : ControllerBase
{
    private readonly BlobServiceClient _svc;
    public BlobServiceController(BlobServiceClient svc) => _svc = svc;

    /// <summary>GET /{account}?comp=list — List Containers</summary>
    [HttpGet]
    [QueryConstraint("comp", "list")]
    public IActionResult ListContainers(string account, [FromQuery] string? prefix,
        [FromQuery] string? marker, [FromQuery] int? maxresults)
    {
        var endpoint = $"{Request.Scheme}://{Request.Host}/{account}";
        var allItems = new List<(string Name, DateTimeOffset LastModified, string ETag)>();

        foreach (var c in _svc.GetBlobContainers(prefix: prefix))
        {
            allItems.Add((c.Name,
                c.Properties.LastModified,
                c.Properties.ETag.ToString()));
        }

        // Apply marker-based pagination
        var (page, nextMarker) = PaginationHelper.Paginate(allItems, marker, maxresults, i => i.Name);
        return Content(XmlHelper.ListContainersXml(endpoint, page, nextMarker), "application/xml");
    }
}
