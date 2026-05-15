using System.Text;
using System.Xml.Linq;

namespace Iciclecreek.Azure.Storage.Server.Infrastructure;

/// <summary>
/// Helpers to build Azure Storage-compatible XML response bodies.
/// </summary>
public static class XmlHelper
{
    public static string ListContainersXml(string serviceEndpoint,
        IEnumerable<(string Name, DateTimeOffset LastModified, string ETag)> containers,
        string? nextMarker = null)
    {
        var xe = new XElement("EnumerationResults",
            new XAttribute("ServiceEndpoint", serviceEndpoint),
            new XElement("Containers",
                containers.Select(c => new XElement("Container",
                    new XElement("Name", c.Name),
                    new XElement("Properties",
                        new XElement("Last-Modified", c.LastModified.ToString("R")),
                        new XElement("Etag", c.ETag),
                        new XElement("LeaseStatus", "unlocked"),
                        new XElement("LeaseState", "available"))))),
            new XElement("NextMarker", nextMarker ?? ""));
        return Declaration + xe.ToString();
    }

    public static string ListBlobsXml(string serviceEndpoint, string containerName,
        IEnumerable<(string Name, long ContentLength, string? ContentType, string? ETag, DateTimeOffset LastModified)> blobs,
        string? prefix = null, string? delimiter = null, string? nextMarker = null)
    {
        var xe = new XElement("EnumerationResults",
            new XAttribute("ServiceEndpoint", serviceEndpoint),
            new XAttribute("ContainerName", containerName),
            prefix != null ? new XElement("Prefix", prefix) : null!,
            delimiter != null ? new XElement("Delimiter", delimiter) : null!,
            new XElement("Blobs",
                blobs.Select(b => new XElement("Blob",
                    new XElement("Name", b.Name),
                    new XElement("Properties",
                        new XElement("Content-Length", b.ContentLength),
                        new XElement("Content-Type", b.ContentType ?? "application/octet-stream"),
                        new XElement("Etag", b.ETag ?? ""),
                        new XElement("Last-Modified", b.LastModified.ToString("R")),
                        new XElement("BlobType", "BlockBlob"),
                        new XElement("LeaseStatus", "unlocked"),
                        new XElement("LeaseState", "available"))))),
            new XElement("NextMarker", nextMarker ?? ""));
        return Declaration + xe.ToString();
    }

    public static string BlockListXml(IEnumerable<(string Id, long Size)> committed, IEnumerable<(string Id, long Size)> uncommitted)
    {
        var xe = new XElement("BlockList",
            new XElement("CommittedBlocks",
                committed.Select(b => new XElement("Block",
                    new XElement("Name", b.Id),
                    new XElement("Size", b.Size)))),
            new XElement("UncommittedBlocks",
                uncommitted.Select(b => new XElement("Block",
                    new XElement("Name", b.Id),
                    new XElement("Size", b.Size)))));
        return Declaration + xe.ToString();
    }

    public static string ListQueuesXml(string serviceEndpoint, IEnumerable<string> queueNames,
        string? nextMarker = null)
    {
        var xe = new XElement("EnumerationResults",
            new XAttribute("ServiceEndpoint", serviceEndpoint),
            new XElement("Queues",
                queueNames.Select(q => new XElement("Queue",
                    new XElement("Name", q)))),
            new XElement("NextMarker", nextMarker ?? ""));
        return Declaration + xe.ToString();
    }

    public static string QueueMessagesXml(IEnumerable<(string MessageId, string PopReceipt, string MessageText,
        DateTimeOffset InsertionTime, DateTimeOffset ExpirationTime, DateTimeOffset TimeNextVisible, int DequeueCount)> messages)
    {
        var xe = new XElement("QueueMessagesList",
            messages.Select(m => new XElement("QueueMessage",
                new XElement("MessageId", m.MessageId),
                m.PopReceipt != null ? new XElement("PopReceipt", m.PopReceipt) : null!,
                new XElement("InsertionTime", m.InsertionTime.ToString("R")),
                new XElement("ExpirationTime", m.ExpirationTime.ToString("R")),
                new XElement("TimeNextVisible", m.TimeNextVisible.ToString("R")),
                new XElement("DequeueCount", m.DequeueCount),
                new XElement("MessageText", m.MessageText))));
        return Declaration + xe.ToString();
    }

    public static string PeekedMessagesXml(IEnumerable<(string MessageId, string MessageText,
        DateTimeOffset InsertionTime, DateTimeOffset ExpirationTime, int DequeueCount)> messages)
    {
        var xe = new XElement("QueueMessagesList",
            messages.Select(m => new XElement("QueueMessage",
                new XElement("MessageId", m.MessageId),
                new XElement("InsertionTime", m.InsertionTime.ToString("R")),
                new XElement("ExpirationTime", m.ExpirationTime.ToString("R")),
                new XElement("DequeueCount", m.DequeueCount),
                new XElement("MessageText", m.MessageText))));
        return Declaration + xe.ToString();
    }

    private const string Declaration = "<?xml version=\"1.0\" encoding=\"utf-8\"?>";
}
