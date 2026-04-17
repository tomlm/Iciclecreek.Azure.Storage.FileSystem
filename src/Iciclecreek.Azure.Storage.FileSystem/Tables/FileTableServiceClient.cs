using System.Linq.Expressions;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Tables;

public class FileTableServiceClient : TableServiceClient
{
    internal readonly FileStorageAccount _account;

    public FileTableServiceClient(string connectionString, FileStorageProvider provider) : base()
    {
        _account = ConnectionStringParser.ResolveAccount(connectionString, provider);
    }

    public FileTableServiceClient(Uri serviceUri, FileStorageProvider provider) : base()
    {
        var name = Iciclecreek.Azure.Storage.FileSystem.Internal.StorageUriParser.ExtractAccountName(serviceUri, provider.HostnameSuffix)
            ?? throw new ArgumentException("Cannot determine account name from URI.", nameof(serviceUri));
        _account = provider.GetAccount(name);
    }

    internal FileTableServiceClient(FileStorageAccount account) : base()
    {
        _account = account;
    }

    public static FileTableServiceClient FromAccount(FileStorageAccount account) => new(account);

    public override string AccountName => _account.Name;
    public override Uri Uri => _account.TableServiceUri;

    // ---- GetTableClient ----

    public override TableClient GetTableClient(string tableName) => new FileTableClient(_account, tableName);

    // ---- CreateTable ----

    public override Response<TableItem> CreateTable(string tableName, CancellationToken cancellationToken = default)
    {
        var client = new FileTableClient(_account, tableName);
        return client.Create(cancellationToken);
    }

    public override Response<TableItem> CreateTableIfNotExists(string tableName, CancellationToken cancellationToken = default)
    {
        var client = new FileTableClient(_account, tableName);
        return client.CreateIfNotExists(cancellationToken);
    }

    public override Response DeleteTable(string tableName, CancellationToken cancellationToken = default)
    {
        var client = new FileTableClient(_account, tableName);
        return client.Delete(cancellationToken);
    }

    public override async Task<Response<TableItem>> CreateTableAsync(string tableName, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateTable(tableName, cancellationToken); }

    public override async Task<Response<TableItem>> CreateTableIfNotExistsAsync(string tableName, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateTableIfNotExists(tableName, cancellationToken); }

    public override async Task<Response> DeleteTableAsync(string tableName, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteTable(tableName, cancellationToken); }

    // ---- Query ----

    public override Pageable<TableItem> Query(string? filter = null, int? maxPerPage = null, CancellationToken cancellationToken = default)
    {
        var items = new List<TableItem>();
        if (!Directory.Exists(_account.TablesRootPath))
            return new StaticPageable<TableItem>(items);

        foreach (var dir in Directory.EnumerateDirectories(_account.TablesRootPath))
        {
            var name = Path.GetFileName(dir);
            if (name.StartsWith('.') || name.StartsWith('_')) continue;
            items.Add(new TableItem(name));
        }
        return new StaticPageable<TableItem>(items);
    }

    public override Pageable<TableItem> Query(Expression<Func<TableItem, bool>> filter, int? maxPerPage = null, CancellationToken cancellationToken = default)
    {
        var all = Query((string?)null, maxPerPage, cancellationToken);
        var compiled = filter.Compile();
        return new StaticPageable<TableItem>(all.Where(compiled).ToList());
    }

    public override AsyncPageable<TableItem> QueryAsync(string? filter = null, int? maxPerPage = null, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<TableItem>(Query(filter, maxPerPage, cancellationToken));

    public override AsyncPageable<TableItem> QueryAsync(Expression<Func<TableItem, bool>> filter, int? maxPerPage = null, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<TableItem>(Query(filter, maxPerPage, cancellationToken));
}
