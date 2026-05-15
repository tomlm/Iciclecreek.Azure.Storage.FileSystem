namespace Iciclecreek.Azure.Storage.FileSystem.Queues.Internal;

/// <summary>
/// JSON-serializable model for a queue message stored on disk.
/// </summary>
internal sealed class QueueMessageFile
{
    public string MessageId { get; set; } = "";
    public string PopReceipt { get; set; } = "";
    public string MessageText { get; set; } = "";
    public DateTimeOffset InsertedOn { get; set; }
    public DateTimeOffset ExpiresOn { get; set; }
    public DateTimeOffset NextVisibleOn { get; set; }
    public int DequeueCount { get; set; }
}
