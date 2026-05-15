using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Iciclecreek.Azure.Storage.Memory.Internal;

namespace Iciclecreek.Azure.Storage.Memory.Queues;

/// <summary>
/// In-memory drop-in replacement for <see cref="QueueServiceClient"/>.
/// Queues are entries in the <see cref="MemoryStorageAccount.Queues"/> dictionary.
/// </summary>
public class MemoryQueueServiceClient : QueueServiceClient
{
    private readonly MemoryStorageAccount _account;

    /// <summary>Initializes a new <see cref="MemoryQueueServiceClient"/> from a <see cref="MemoryStorageAccount"/>.</summary>
    public MemoryQueueServiceClient(MemoryStorageAccount account) : base()
    {
        _account = account;
    }

    /// <summary>Initializes a new <see cref="MemoryQueueServiceClient"/> from a connection string and provider.</summary>
    public MemoryQueueServiceClient(string connectionString, MemoryStorageProvider provider) : base()
    {
        _account = ConnectionStringParser.ResolveAccount(connectionString, provider);
    }

    /// <summary>Initializes a new <see cref="MemoryQueueServiceClient"/> from a service URI and provider.</summary>
    public MemoryQueueServiceClient(Uri serviceUri, MemoryStorageProvider provider) : base()
    {
        var name = StorageUriParser.ExtractAccountName(serviceUri, provider.HostnameSuffix)
            ?? throw new ArgumentException("Cannot determine account name from URI.", nameof(serviceUri));
        _account = provider.GetAccount(name);
    }

    /// <summary>Creates a new <see cref="MemoryQueueServiceClient"/> directly from a <see cref="MemoryStorageAccount"/>.</summary>
    public static MemoryQueueServiceClient FromAccount(MemoryStorageAccount account) => new(account);

    // ── Properties ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override string AccountName => _account.Name;
    /// <inheritdoc/>
    public override Uri Uri => _account.QueueServiceUri;

    // ── GetQueueClient ──────────────────────────────────────────────────

    /// <inheritdoc/>
    public override QueueClient GetQueueClient(string queueName) => new MemoryQueueClient(_account, queueName);

    // ── CreateQueue ─────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<QueueClient> CreateQueue(string queueName, IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        var client = new MemoryQueueClient(_account, queueName);
        client.Create(metadata, cancellationToken);
        return Response.FromValue<QueueClient>(client, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override async Task<Response<QueueClient>> CreateQueueAsync(string queueName, IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        return CreateQueue(queueName, metadata, cancellationToken);
    }

    // ── DeleteQueue ─────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response DeleteQueue(string queueName, CancellationToken cancellationToken = default)
    {
        var client = new MemoryQueueClient(_account, queueName);
        client.Delete(cancellationToken);
        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override async Task<Response> DeleteQueueAsync(string queueName, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        return DeleteQueue(queueName, cancellationToken);
    }

    // ── GetQueues ───────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Pageable<QueueItem> GetQueues(QueueTraits traits = QueueTraits.None, string? prefix = null, CancellationToken cancellationToken = default)
    {
        var items = new List<QueueItem>();
        foreach (var kvp in _account.Queues.OrderBy(k => k.Key, StringComparer.Ordinal))
        {
            if (prefix != null && !kvp.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                continue;
            items.Add(QueuesModelFactory.QueueItem(kvp.Key, metadata: kvp.Value.Metadata));
        }
        return new StaticPageable<QueueItem>(items);
    }

    /// <inheritdoc/>
    public override AsyncPageable<QueueItem> GetQueuesAsync(QueueTraits traits = QueueTraits.None, string? prefix = null, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<QueueItem>(GetQueues(traits, prefix, cancellationToken));

    // ── Service Properties (stubs) ──────────────────────────────────────

    /// <inheritdoc/>
    public override Response<QueueServiceProperties> GetProperties(CancellationToken cancellationToken = default)
        => Response.FromValue(new QueueServiceProperties(), StubResponse.Ok());

    /// <inheritdoc/>
    public override async Task<Response<QueueServiceProperties>> GetPropertiesAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetProperties(cancellationToken); }

    /// <inheritdoc/>
    public override Response SetProperties(QueueServiceProperties properties, CancellationToken cancellationToken = default)
        => StubResponse.Ok();

    /// <inheritdoc/>
    public override async Task<Response> SetPropertiesAsync(QueueServiceProperties properties, CancellationToken cancellationToken = default)
    { await Task.Yield(); return SetProperties(properties, cancellationToken); }
}
