using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Queues;

/// <summary>
/// Filesystem-backed drop-in replacement for <see cref="QueueServiceClient"/>.
/// </summary>
public class FileQueueServiceClient : QueueServiceClient
{
    private readonly FileStorageAccount _account;

    internal FileQueueServiceClient(FileStorageAccount account) : base()
    {
        _account = account;
    }

    public FileQueueServiceClient(string connectionString, FileStorageProvider provider) : base()
    {
        _account = ConnectionStringParser.ResolveAccount(connectionString, provider);
    }

    public FileQueueServiceClient(Uri serviceUri, FileStorageProvider provider) : base()
    {
        var accountName = StorageUriParser.ExtractAccountName(serviceUri, provider.HostnameSuffix)
            ?? throw new InvalidOperationException("Cannot extract account name from URI.");
        _account = provider.GetAccount(accountName);
    }

    public static FileQueueServiceClient FromAccount(FileStorageAccount account) => new(account);

    // ── Properties ──────────────────────────────────────────────────────

    public override string AccountName => _account.Name;
    public override Uri Uri => _account.QueueServiceUri;

    // ── GetQueueClient ──────────────────────────────────────────────────

    public override QueueClient GetQueueClient(string queueName) => new FileQueueClient(_account, queueName);

    // ── CreateQueue ─────────────────────────────────────────────────────

    public override Response<QueueClient> CreateQueue(string queueName, IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        var client = new FileQueueClient(_account, queueName);
        client.Create(metadata, cancellationToken);
        return Response.FromValue<QueueClient>(client, StubResponse.Created());
    }

    public override async Task<Response<QueueClient>> CreateQueueAsync(string queueName, IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        return CreateQueue(queueName, metadata, cancellationToken);
    }

    // ── DeleteQueue ─────────────────────────────────────────────────────

    public override Response DeleteQueue(string queueName, CancellationToken cancellationToken = default)
    {
        var client = new FileQueueClient(_account, queueName);
        client.Delete(cancellationToken);
        return StubResponse.NoContent();
    }

    public override async Task<Response> DeleteQueueAsync(string queueName, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        return DeleteQueue(queueName, cancellationToken);
    }

    // ── GetQueues ───────────────────────────────────────────────────────

    public override Pageable<QueueItem> GetQueues(QueueTraits traits = QueueTraits.None, string? prefix = null, CancellationToken cancellationToken = default)
    {
        var queuesRoot = _account.QueuesRootPath;
        if (!Directory.Exists(queuesRoot))
            return new StaticPageable<QueueItem>(Array.Empty<QueueItem>());

        var items = Directory.EnumerateDirectories(queuesRoot)
            .Select(d => Path.GetFileName(d))
            .Where(n => !string.IsNullOrEmpty(n) && !n.StartsWith('.') && !n.StartsWith('_'))
            .Where(n => prefix == null || n.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            .OrderBy(n => n, StringComparer.Ordinal)
            .Select(n => QueuesModelFactory.QueueItem(n, metadata: null))
            .ToList();

        return new StaticPageable<QueueItem>(items);
    }

    public override AsyncPageable<QueueItem> GetQueuesAsync(QueueTraits traits = QueueTraits.None, string? prefix = null, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<QueueItem>(GetQueues(traits, prefix, cancellationToken));

    // ── Service Properties (stubs) ──────────────────────────────────────

    public override Response<QueueServiceProperties> GetProperties(CancellationToken cancellationToken = default)
        => Response.FromValue(new QueueServiceProperties(), StubResponse.Ok());

    public override async Task<Response<QueueServiceProperties>> GetPropertiesAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetProperties(cancellationToken); }

    public override Response SetProperties(QueueServiceProperties properties, CancellationToken cancellationToken = default)
        => StubResponse.Ok();

    public override async Task<Response> SetPropertiesAsync(QueueServiceProperties properties, CancellationToken cancellationToken = default)
    { await Task.Yield(); return SetProperties(properties, cancellationToken); }
}
