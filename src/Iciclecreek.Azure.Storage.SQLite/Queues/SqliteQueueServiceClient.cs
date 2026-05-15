using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Iciclecreek.Azure.Storage.SQLite.Internal;

namespace Iciclecreek.Azure.Storage.SQLite.Queues;

/// <summary>
/// SQLite-backed drop-in replacement for <see cref="QueueServiceClient"/>.
/// Queues are rows in the Queues table of the SQLite database.
/// </summary>
public class SqliteQueueServiceClient : QueueServiceClient
{
    private readonly SqliteStorageAccount _account;

    /// <summary>Initializes a new <see cref="SqliteQueueServiceClient"/> from a <see cref="SqliteStorageAccount"/>.</summary>
    public SqliteQueueServiceClient(SqliteStorageAccount account) : base()
    {
        _account = account;
    }

    /// <summary>Creates a new <see cref="SqliteQueueServiceClient"/> directly from a <see cref="SqliteStorageAccount"/>.</summary>
    public static SqliteQueueServiceClient FromAccount(SqliteStorageAccount account) => new(account);

    // ── Properties ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override string AccountName => _account.Name;
    /// <inheritdoc/>
    public override Uri Uri => _account.QueueServiceUri;

    // ── GetQueueClient ──────────────────────────────────────────────────

    /// <inheritdoc/>
    public override QueueClient GetQueueClient(string queueName) => new SqliteQueueClient(_account, queueName);

    // ── CreateQueue ─────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<QueueClient> CreateQueue(string queueName, IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        var client = new SqliteQueueClient(_account, queueName);
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
        var client = new SqliteQueueClient(_account, queueName);
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
        using var conn = _account.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Name FROM Queues ORDER BY Name";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var name = reader.GetString(0);
            if (prefix != null && !name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                continue;
            items.Add(QueuesModelFactory.QueueItem(name, metadata: null));
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
