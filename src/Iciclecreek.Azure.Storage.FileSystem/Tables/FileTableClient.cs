using System.Linq.Expressions;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Internal;
using Iciclecreek.Azure.Storage.FileSystem.Tables.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Tables;

public class FileTableClient : TableClient
{
    internal readonly TableStore _store;
    internal readonly FileStorageAccount _account;

    public FileTableClient(string connectionString, string tableName, FileStorageProvider provider) : base()
    {
        _account = ConnectionStringParser.ResolveAccount(connectionString, provider);
        _store = new TableStore(_account, tableName);
    }

    public FileTableClient(Uri tableUri, FileStorageProvider provider) : base()
    {
        var (acctName, table) = Iciclecreek.Azure.Storage.FileSystem.Internal.StorageUriParser.ParseTableUri(tableUri, provider.HostnameSuffix);
        _account = provider.GetAccount(acctName);
        _store = new TableStore(_account, table);
    }

    internal FileTableClient(FileStorageAccount account, string tableName) : base()
    {
        _account = account;
        _store = new TableStore(account, tableName);
    }

    public static FileTableClient FromAccount(FileStorageAccount account, string tableName) => new(account, tableName);

    public override string Name => _store.TableName;
    public override string AccountName => _account.Name;
    public override Uri Uri => new($"{_account.TableServiceUri}{_store.TableName}");

    // ---- Create / Delete (no file I/O, just directory ops) ----

    public override Response<TableItem> Create(CancellationToken cancellationToken = default)
    {
        if (!_store.CreateTable())
            throw new RequestFailedException(409, "Table already exists.", "TableAlreadyExists", null);
        return Response.FromValue(new TableItem(_store.TableName), StubResponse.Created());
    }

    public override Response<TableItem> CreateIfNotExists(CancellationToken cancellationToken = default)
    {
        _store.CreateTable();
        return Response.FromValue(new TableItem(_store.TableName), StubResponse.Ok());
    }

    public override Response Delete(CancellationToken cancellationToken = default)
    {
        if (!_store.DeleteTable())
            throw new RequestFailedException(404, "Table not found.", "ResourceNotFound", null);
        return StubResponse.NoContent();
    }

    public override async Task<Response<TableItem>> CreateAsync(CancellationToken cancellationToken = default)
        => Create(cancellationToken);
    public override async Task<Response<TableItem>> CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
        => CreateIfNotExists(cancellationToken);
    public override async Task<Response> DeleteAsync(CancellationToken cancellationToken = default)
        => Delete(cancellationToken);

    // ---- Entity CRUD (async = primary) ----

    public override async Task<Response> AddEntityAsync<T>(T entity, CancellationToken cancellationToken = default)
    {
        await _store.AddEntityAsync(entity, cancellationToken).ConfigureAwait(false);
        return StubResponse.NoContent();
    }

    public override Response AddEntity<T>(T entity, CancellationToken cancellationToken = default)
        => AddEntityAsync(entity, cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response<T>> GetEntityAsync<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var entity = await _store.GetEntityAsync(partitionKey, rowKey, cancellationToken).ConfigureAwait(false);
        return Response.FromValue(ConvertEntity<T>(entity), StubResponse.Ok());
    }

    public override Response<T> GetEntity<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => GetEntityAsync<T>(partitionKey, rowKey, select, cancellationToken).GetAwaiter().GetResult();

    public override async Task<NullableResponse<T>> GetEntityIfExistsAsync<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        try
        {
            var result = await GetEntityAsync<T>(partitionKey, rowKey, select, cancellationToken).ConfigureAwait(false);
            return Response.FromValue<T>(result.Value, StubResponse.Ok());
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return default!;
        }
    }

    public override NullableResponse<T> GetEntityIfExists<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => GetEntityIfExistsAsync<T>(partitionKey, rowKey, select, cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response> UpsertEntityAsync<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    {
        await _store.UpsertEntityAsync(entity, mode, cancellationToken).ConfigureAwait(false);
        return StubResponse.NoContent();
    }

    public override Response UpsertEntity<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        => UpsertEntityAsync(entity, mode, cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response> UpdateEntityAsync<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    {
        await _store.UpdateEntityAsync(entity, ifMatch, mode, cancellationToken).ConfigureAwait(false);
        return StubResponse.NoContent();
    }

    public override Response UpdateEntity<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        => UpdateEntityAsync(entity, ifMatch, mode, cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response> DeleteEntityAsync(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
    {
        var etag = ifMatch == default ? ETag.All : ifMatch;
        await _store.DeleteEntityAsync(partitionKey, rowKey, etag, cancellationToken).ConfigureAwait(false);
        return StubResponse.NoContent();
    }

    public override Response DeleteEntity(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
        => DeleteEntityAsync(partitionKey, rowKey, ifMatch, cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response> DeleteEntityAsync(ITableEntity entity, ETag ifMatch = default, CancellationToken cancellationToken = default)
        => await DeleteEntityAsync(entity.PartitionKey, entity.RowKey, ifMatch, cancellationToken).ConfigureAwait(false);

    public override Response DeleteEntity(ITableEntity entity, ETag ifMatch = default, CancellationToken cancellationToken = default)
        => DeleteEntityAsync(entity, ifMatch, cancellationToken).GetAwaiter().GetResult();

    // ---- Query (async = primary for entity enumeration) ----

    public override AsyncPageable<T> QueryAsync<T>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var predicate = ODataFilterParser.Parse(filter ?? "");
        return new AsyncEnumerablePageable<T>(_store.EnumerateEntitiesAsync(), e => predicate(e) ? ConvertEntity<T>(e) : default);
    }

    public override Pageable<T> Query<T>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var predicate = ODataFilterParser.Parse(filter ?? "");
        var results = new List<T>();
        var enumerator = _store.EnumerateEntitiesAsync().GetAsyncEnumerator(cancellationToken);
        try { while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult()) { if (predicate(enumerator.Current)) results.Add(ConvertEntity<T>(enumerator.Current)); } }
        finally { enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult(); }
        return new StaticPageable<T>(results);
    }

    public override AsyncPageable<T> QueryAsync<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var compiled = filter.Compile();
        return new AsyncEnumerablePageable<T>(_store.EnumerateEntitiesAsync(), e => { var c = ConvertEntity<T>(e); return compiled(c) ? c : default; });
    }

    public override Pageable<T> Query<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var compiled = filter.Compile();
        var results = new List<T>();
        var enumerator = _store.EnumerateEntitiesAsync().GetAsyncEnumerator(cancellationToken);
        try { while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult()) { var c = ConvertEntity<T>(enumerator.Current); if (compiled(c)) results.Add(c); } }
        finally { enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult(); }
        return new StaticPageable<T>(results);
    }

    // ---- SubmitTransaction (async = primary) ----

    public override async Task<Response<IReadOnlyList<Response>>> SubmitTransactionAsync(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
    {
        var actions = transactionActions.ToList();
        if (actions.Count == 0)
            throw new ArgumentException("At least one action is required.", nameof(transactionActions));

        var pk = actions[0].Entity.PartitionKey;
        for (var i = 1; i < actions.Count; i++)
        {
            if (actions[i].Entity.PartitionKey != pk)
                throw new RequestFailedException(400, "All entities in a transaction must have the same PartitionKey.", "InvalidInput", null);
        }

        var snapshots = new Dictionary<string, string?>();
        foreach (var action in actions)
        {
            var path = _store.EntityPath(action.Entity.PartitionKey, action.Entity.RowKey);
            if (!snapshots.ContainsKey(path))
                snapshots[path] = File.Exists(path) ? await File.ReadAllTextAsync(path, cancellationToken).ConfigureAwait(false) : null;
        }

        var responses = new List<Response>();
        try
        {
            for (var i = 0; i < actions.Count; i++)
            {
                var a = actions[i];
                try
                {
                    switch (a.ActionType)
                    {
                        case TableTransactionActionType.Add:
                            await _store.AddEntityAsync(a.Entity, cancellationToken).ConfigureAwait(false);
                            break;
                        case TableTransactionActionType.UpdateMerge:
                            await _store.UpdateEntityAsync(a.Entity, a.ETag, TableUpdateMode.Merge, cancellationToken).ConfigureAwait(false);
                            break;
                        case TableTransactionActionType.UpdateReplace:
                            await _store.UpdateEntityAsync(a.Entity, a.ETag, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
                            break;
                        case TableTransactionActionType.UpsertMerge:
                            await _store.UpsertEntityAsync(a.Entity, TableUpdateMode.Merge, cancellationToken).ConfigureAwait(false);
                            break;
                        case TableTransactionActionType.UpsertReplace:
                            await _store.UpsertEntityAsync(a.Entity, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
                            break;
                        case TableTransactionActionType.Delete:
                            await _store.DeleteEntityAsync(a.Entity.PartitionKey, a.Entity.RowKey, a.ETag == default ? ETag.All : a.ETag, cancellationToken).ConfigureAwait(false);
                            break;
                    }
                    responses.Add(StubResponse.NoContent());
                }
                catch (RequestFailedException ex)
                {
                    await RollbackAsync(snapshots, cancellationToken).ConfigureAwait(false);
                    throw new TableTransactionFailedException(ex);
                }
            }
        }
        catch (TableTransactionFailedException) { throw; }
        catch (Exception ex)
        {
            await RollbackAsync(snapshots, cancellationToken).ConfigureAwait(false);
            throw new RequestFailedException(500, ex.Message, null, ex);
        }

        IReadOnlyList<Response> list = responses;
        return Response.FromValue(list, StubResponse.Accepted());
    }

    public override Response<IReadOnlyList<Response>> SubmitTransaction(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
        => SubmitTransactionAsync(transactionActions, cancellationToken).GetAwaiter().GetResult();

    // ---- Helpers ----

    private static async Task RollbackAsync(Dictionary<string, string?> snapshots, CancellationToken ct)
    {
        foreach (var kvp in snapshots)
        {
            if (kvp.Value is null)
            {
                if (File.Exists(kvp.Key)) File.Delete(kvp.Key);
            }
            else
            {
                await AtomicFile.WriteAllTextAsync(kvp.Key, kvp.Value, ct).ConfigureAwait(false);
            }
        }
    }

    private static T ConvertEntity<T>(TableEntity entity) where T : class, ITableEntity
    {
        if (typeof(T) == typeof(TableEntity))
            return (entity as T)!;

        var result = (T)Activator.CreateInstance(typeof(T))!;
        result.PartitionKey = entity.PartitionKey;
        result.RowKey = entity.RowKey;
        result.Timestamp = entity.Timestamp;

        if (entity.TryGetValue("odata.etag", out var etagObj) && etagObj is string etagStr)
            result.ETag = new ETag(etagStr);

        var type = typeof(T);
        foreach (var kvp in entity)
        {
            if (kvp.Key is "PartitionKey" or "RowKey" or "Timestamp" or "odata.etag") continue;
            var prop = type.GetProperty(kvp.Key);
            if (prop is not null && prop.CanWrite && kvp.Value is not null)
            {
                try { prop.SetValue(result, Convert.ChangeType(kvp.Value, prop.PropertyType)); }
                catch { }
            }
        }
        return result;
    }
}
