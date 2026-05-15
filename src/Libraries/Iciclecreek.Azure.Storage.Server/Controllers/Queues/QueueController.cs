using System.Xml.Linq;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Iciclecreek.Azure.Storage.Server.Infrastructure;
using Microsoft.AspNetCore.Mvc;

namespace Iciclecreek.Azure.Storage.Server.Controllers.Queues;

/// <summary>
/// Queue-level operations: create, delete, metadata, and message operations.
/// </summary>
[ApiController]
[ServicePortConstraint("Queue", 10001)]
[Route("{account}/{queue}")]
public class QueueController : ControllerBase
{
    private readonly QueueServiceClient _svc;
    public QueueController(QueueServiceClient svc) => _svc = svc;

    // ── Queue CRUD ──────────────────────────────────────────────────────

    /// <summary>PUT /{account}/{queue} — Create Queue</summary>
    [HttpPut]
    [QueryAbsentConstraint("comp")]
    public async Task<IActionResult> CreateQueue(string account, string queue)
    {
        var client = _svc.GetQueueClient(queue);
        await client.CreateIfNotExistsAsync();
        return StatusCode(201);
    }

    /// <summary>DELETE /{account}/{queue} — Delete Queue</summary>
    [HttpDelete]
    [QueryAbsentConstraint("comp")]
    public async Task<IActionResult> DeleteQueue(string account, string queue)
    {
        var client = _svc.GetQueueClient(queue);
        await client.DeleteIfExistsAsync();
        return StatusCode(204);
    }

    /// <summary>GET /{account}/{queue}?comp=metadata — Get Queue Metadata</summary>
    [HttpGet]
    [QueryConstraint("comp", "metadata")]
    public async Task<IActionResult> GetQueueMetadata(string account, string queue)
    {
        var client = _svc.GetQueueClient(queue);
        var props = await client.GetPropertiesAsync();

        Response.Headers["x-ms-approximate-messages-count"] = props.Value.ApproximateMessagesCount.ToString();
        foreach (var kv in props.Value.Metadata)
            Response.Headers[$"x-ms-meta-{kv.Key}"] = kv.Value;

        return Ok();
    }

    /// <summary>PUT /{account}/{queue}?comp=metadata — Set Queue Metadata</summary>
    [HttpPut]
    [QueryConstraint("comp", "metadata")]
    public async Task<IActionResult> SetQueueMetadata(string account, string queue)
    {
        var client = _svc.GetQueueClient(queue);
        var metadata = new Dictionary<string, string>();
        foreach (var header in Request.Headers)
        {
            if (header.Key.StartsWith("x-ms-meta-", StringComparison.OrdinalIgnoreCase))
                metadata[header.Key.Substring("x-ms-meta-".Length)] = header.Value.ToString();
        }
        await client.SetMetadataAsync(metadata);
        return StatusCode(204);
    }

    // ── Messages ────────────────────────────────────────────────────────

    /// <summary>POST /{account}/{queue}/messages — Put Message</summary>
    [HttpPost("messages")]
    public async Task<IActionResult> PutMessage(string account, string queue)
    {
        var client = _svc.GetQueueClient(queue);

        // Parse XML body: <QueueMessage><MessageText>...</MessageText></QueueMessage>
        var doc = await XDocument.LoadAsync(Request.Body, LoadOptions.None, HttpContext.RequestAborted);
        var messageText = doc.Root?.Element("MessageText")?.Value ?? "";

        TimeSpan? visibilityTimeout = null;
        if (Request.Query.TryGetValue("visibilitytimeout", out var vtStr) && int.TryParse(vtStr, out var vt))
            visibilityTimeout = TimeSpan.FromSeconds(vt);

        TimeSpan? timeToLive = null;
        if (Request.Query.TryGetValue("messagettl", out var ttlStr) && int.TryParse(ttlStr, out var ttl))
            timeToLive = TimeSpan.FromSeconds(ttl);

        var receipt = await client.SendMessageAsync(messageText, visibilityTimeout, timeToLive);

        var xml = XmlHelper.QueueMessagesXml(new[]
        {
            (receipt.Value.MessageId,
             receipt.Value.PopReceipt,
             messageText,
             receipt.Value.InsertionTime,
             receipt.Value.ExpirationTime,
             receipt.Value.TimeNextVisible,
             0)
        });

        return Content(xml, "application/xml");
    }

    /// <summary>GET /{account}/{queue}/messages — Get Messages (dequeue) or Peek</summary>
    [HttpGet("messages")]
    public async Task<IActionResult> GetMessages(string account, string queue,
        [FromQuery] bool peekonly = false,
        [FromQuery] int? numofmessages = null,
        [FromQuery] int? visibilitytimeout = null)
    {
        var client = _svc.GetQueueClient(queue);
        var count = numofmessages ?? 1;

        if (peekonly)
        {
            var peeked = await client.PeekMessagesAsync(count);
            var items = peeked.Value.Select(m =>
                (MessageId: m.MessageId,
                 MessageText: m.Body.ToString(),
                 InsertionTime: m.InsertedOn.GetValueOrDefault(),
                 ExpirationTime: m.ExpiresOn.GetValueOrDefault(),
                 DequeueCount: (int)m.DequeueCount));
            return Content(XmlHelper.PeekedMessagesXml(items), "application/xml");
        }
        else
        {
            var visTimeout = visibilitytimeout.HasValue
                ? TimeSpan.FromSeconds(visibilitytimeout.Value)
                : (TimeSpan?)null;

            var received = await client.ReceiveMessagesAsync(count, visTimeout);
            var items = received.Value.Select(m =>
                (MessageId: m.MessageId,
                 PopReceipt: m.PopReceipt,
                 MessageText: m.Body.ToString(),
                 InsertionTime: m.InsertedOn.GetValueOrDefault(),
                 ExpirationTime: m.ExpiresOn.GetValueOrDefault(),
                 TimeNextVisible: m.NextVisibleOn.GetValueOrDefault(),
                 DequeueCount: (int)m.DequeueCount));
            return Content(XmlHelper.QueueMessagesXml(items), "application/xml");
        }
    }

    /// <summary>DELETE /{account}/{queue}/messages/{messageId}?popreceipt=x — Delete Message</summary>
    [HttpDelete("messages/{messageId}")]
    public async Task<IActionResult> DeleteMessage(string account, string queue, string messageId, [FromQuery] string popreceipt)
    {
        var client = _svc.GetQueueClient(queue);
        await client.DeleteMessageAsync(messageId, popreceipt);
        return StatusCode(204);
    }

    /// <summary>DELETE /{account}/{queue}/messages — Clear Messages</summary>
    [HttpDelete("messages")]
    public async Task<IActionResult> ClearMessages(string account, string queue)
    {
        var client = _svc.GetQueueClient(queue);
        await client.ClearMessagesAsync();
        return StatusCode(204);
    }

    /// <summary>PUT /{account}/{queue}/messages/{messageId}?popreceipt=x&amp;visibilitytimeout=y — Update Message</summary>
    [HttpPut("messages/{messageId}")]
    public async Task<IActionResult> UpdateMessage(string account, string queue, string messageId,
        [FromQuery] string popreceipt, [FromQuery] int visibilitytimeout)
    {
        var client = _svc.GetQueueClient(queue);

        string? messageText = null;
        if (Request.ContentLength > 0)
        {
            var doc = await XDocument.LoadAsync(Request.Body, LoadOptions.None, HttpContext.RequestAborted);
            messageText = doc.Root?.Element("MessageText")?.Value;
        }

        var receipt = await client.UpdateMessageAsync(
            messageId, popreceipt,
            messageText,
            TimeSpan.FromSeconds(visibilitytimeout));

        Response.Headers["x-ms-popreceipt"] = receipt.Value.PopReceipt;
        Response.Headers["x-ms-time-next-visible"] = receipt.Value.NextVisibleOn.ToString("R");
        return StatusCode(204);
    }
}
