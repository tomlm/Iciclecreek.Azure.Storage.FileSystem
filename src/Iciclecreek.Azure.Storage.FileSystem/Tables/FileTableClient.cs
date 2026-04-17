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

    // ---- Create / Delete ----

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
    { await Task.Yield(); return Create(cancellationToken); }

    public override async Task<Response<TableItem>> CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateIfNotExists(cancellationToken); }

    public override async Task<Response> DeleteAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return Delete(cancellationToken); }

    // ---- Entity CRUD ----

    public override Response AddEntity<T>(T entity, CancellationToken cancellationToken = default)
    {
        _store.AddEntity(entity);
        return StubResponse.NoContent();
    }

    public override async Task<Response> AddEntityAsync<T>(T entity, CancellationToken cancellationToken = default)
    { await Task.Yield(); return AddEntity(entity, cancellationToken); }

    public override Response<T> GetEntity<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var entity = _store.GetEntity(partitionKey, rowKey);
        var result = ConvertEntity<T>(entity);
        return Response.FromValue(result, StubResponse.Ok());
    }

    public override async Task<Response<T>> GetEntityAsync<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetEntity<T>(partitionKey, rowKey, select, cancellationToken); }

    public override NullableResponse<T> GetEntityIfExists<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        try
        {
            var result = GetEntity<T>(partitionKey, rowKey, select, cancellationToken);
            return Response.FromValue<T>(result.Value, StubResponse.Ok());
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return default!;
        }
    }

    public override async Task<NullableResponse<T>> GetEntityIfExistsAsync<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetEntityIfExists<T>(partitionKey, rowKey, select, cancellationToken); }

    public override Response UpsertEntity<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    {
        _store.UpsertEntity(entity, mode);
        return StubResponse.NoContent();
    }

    public override async Task<Response> UpsertEntityAsync<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    { await Task.Yield(); return UpsertEntity(entity, mode, cancellationToken); }

    public override Response UpdateEntity<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    {
        _store.UpdateEntity(entity, ifMatch, mode);
        return StubResponse.NoContent();
    }

    public override async Task<Response> UpdateEntityAsync<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    { await Task.Yield(); return UpdateEntity(entity, ifMatch, mode, cancellationToken); }

    public override Response DeleteEntity(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
    {
        var etag = ifMatch == default ? ETag.All : ifMatch;
        _store.DeleteEntity(partitionKey, rowKey, etag);
        return StubResponse.NoContent();
    }

    public override Response DeleteEntity(ITableEntity entity, ETag ifMatch = default, CancellationToken cancellationToken = default)
        => DeleteEntity(entity.PartitionKey, entity.RowKey, ifMatch, cancellationToken);

    public override async Task<Response> DeleteEntityAsync(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteEntity(partitionKey, rowKey, ifMatch, cancellationToken); }

    public override async Task<Response> DeleteEntityAsync(ITableEntity entity, ETag ifMatch = default, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteEntity(entity, ifMatch, cancellationToken); }

    // ---- Query ----

    public override Pageable<T> Query<T>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var predicate = ODataFilterParser.Parse(filter ?? "");
        var results = _store.EnumerateEntities().Where(e => predicate(e)).Select(e => ConvertEntity<T>(e)).ToList();
        return new StaticPageable<T>(results);
    }

    public override Pageable<T> Query<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var compiled = filter.Compile();
        var results = _store.EnumerateEntities().Select(e => ConvertEntity<T>(e)).Where(e => compiled(e)).ToList();
        return new StaticPageable<T>(results);
    }

    public override AsyncPageable<T> QueryAsync<T>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<T>(Query<T>(filter, maxPerPage, select, cancellationToken));

    public override AsyncPageable<T> QueryAsync<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<T>(Query(filter, maxPerPage, select, cancellationToken));

    // ---- SubmitTransaction ----

    public override Response<IReadOnlyList<Response>> SubmitTransaction(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
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

        // Snapshot current state for rollback.
        var snapshots = new Dictionary<string, string?>();
        foreach (var action in actions)
        {
            var path = _store.EntityPath(action.Entity.PartitionKey, action.Entity.RowKey);
            if (!snapshots.ContainsKey(path))
                snapshots[path] = File.Exists(path) ? File.ReadAllText(path) : null;
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
                            _store.AddEntity(a.Entity);
                            break;
                        case TableTransactionActionType.UpdateMerge:
                            _store.UpdateEntity(a.Entity, a.ETag, TableUpdateMode.Merge);
                            break;
                        case TableTransactionActionType.UpdateReplace:
                            _store.UpdateEntity(a.Entity, a.ETag, TableUpdateMode.Replace);
                            break;
                        case TableTransactionActionType.UpsertMerge:
                            _store.UpsertEntity(a.Entity, TableUpdateMode.Merge);
                            break;
                        case TableTransactionActionType.UpsertReplace:
                            _store.UpsertEntity(a.Entity, TableUpdateMode.Replace);
                            break;
                        case TableTransactionActionType.Delete:
                            _store.DeleteEntity(a.Entity.PartitionKey, a.Entity.RowKey, a.ETag == default ? ETag.All : a.ETag);
                            break;
                    }
                    responses.Add(StubResponse.NoContent());
                }
                catch (RequestFailedException ex)
                {
                    // Rollback
                    foreach (var kvp in snapshots)
                    {
                        if (kvp.Value is null)
                        {
                            if (File.Exists(kvp.Key)) File.Delete(kvp.Key);
                        }
                        else
                        {
                            AtomicFile.WriteAllText(kvp.Key, kvp.Value);
                        }
                    }
                    throw new TableTransactionFailedException(ex);
                }
            }
        }
        catch (TableTransactionFailedException) { throw; }
        catch (Exception ex)
        {
            // Rollback on unexpected errors.
            foreach (var kvp in snapshots)
            {
                if (kvp.Value is null)
                {
                    if (File.Exists(kvp.Key)) File.Delete(kvp.Key);
                }
                else
                {
                    AtomicFile.WriteAllText(kvp.Key, kvp.Value);
                }
            }
            throw new RequestFailedException(500, ex.Message, null, ex);
        }

        IReadOnlyList<Response> list = responses;
        return Response.FromValue(list, StubResponse.Accepted());
    }

    public override async Task<Response<IReadOnlyList<Response>>> SubmitTransactionAsync(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
    { await Task.Yield(); return SubmitTransaction(transactionActions, cancellationToken); }

    // ---- Helpers ----

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
                try
                {
                    prop.SetValue(result, Convert.ChangeType(kvp.Value, prop.PropertyType));
                }
                catch { /* property type mismatch — skip */ }
            }
        }
        return result;
    }
}
