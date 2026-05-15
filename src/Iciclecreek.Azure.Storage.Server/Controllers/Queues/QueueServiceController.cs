using Azure.Storage.Queues;
using Iciclecreek.Azure.Storage.Server.Infrastructure;
using Microsoft.AspNetCore.Mvc;

namespace Iciclecreek.Azure.Storage.Server.Controllers.Queues;

/// <summary>
/// Account-level queue service operations (list queues).
/// </summary>
[ApiController]
[ServicePortConstraint("Queue", 10001)]
[Route("{account}")]
public class QueueServiceController : ControllerBase
{
    private readonly QueueServiceClient _svc;
    public QueueServiceController(QueueServiceClient svc) => _svc = svc;

    /// <summary>GET /{account}?comp=list — List Queues</summary>
    [HttpGet]
    [QueryConstraint("comp", "list")]
    public IActionResult ListQueues(string account, [FromQuery] string? prefix,
        [FromQuery] string? marker, [FromQuery] int? maxresults)
    {
        var endpoint = $"{Request.Scheme}://{Request.Host}/{account}";
        var allNames = new List<string>();

        foreach (var q in _svc.GetQueues(prefix: prefix))
            allNames.Add(q.Name);

        var (page, nextMarker) = PaginationHelper.Paginate(allNames, marker, maxresults, n => n);
        return Content(XmlHelper.ListQueuesXml(endpoint, page, nextMarker), "application/xml");
    }
}
