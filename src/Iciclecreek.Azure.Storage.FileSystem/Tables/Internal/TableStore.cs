using System.Security.Cryptography;
using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Tables.Internal;

internal sealed class TableStore
{
    public TableStore(FileStorageAccount account, string tableName)
    {
        Account = account;
        TableName = tableName;
        TablePath = Path.Combine(account.TablesRootPath, tableName);
    }

    public FileStorageAccount Account { get; }
    public string TableName { get; }
    public string TablePath { get; }
    public FileStorageProvider Provider => Account.Provider;

    public bool TableExists() => Directory.Exists(TablePath);

    public bool CreateTable()
    {
        if (Directory.Exists(TablePath)) return false;
        Directory.CreateDirectory(TablePath);
        return true;
    }

    public bool DeleteTable()
    {
        if (!Directory.Exists(TablePath)) return false;
        Directory.Delete(TablePath, recursive: true);
        return true;
    }

    public string EntityPath(string pk, string rk)
    {
        var encodedPk = TableKeyEncoder.Encode(pk);
        var encodedRk = TableKeyEncoder.Encode(rk);
        return Path.Combine(TablePath, encodedPk, encodedRk + ".json");
    }

    public string GenerateETag()
    {
        Span<byte> randomBytes = stackalloc byte[16];
        RandomNumberGenerator.Fill(randomBytes);
        return $"W/\"datetime'{DateTimeOffset.UtcNow:O}'\"";
    }

    public void AddEntity(ITableEntity entity)
    {
        var path = EntityPath(entity.PartitionKey, entity.RowKey);
        if (File.Exists(path))
            throw new RequestFailedException(409, "Entity already exists.", "EntityAlreadyExists", null);

        var etag = GenerateETag();
        var json = EntitySerializer.Serialize(entity, etag, Provider.JsonSerializerOptions);
        AtomicFile.WriteAllText(path, json);
    }

    public TableEntity GetEntity(string pk, string rk)
    {
        var path = EntityPath(pk, rk);
        if (!File.Exists(path))
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);
        var json = File.ReadAllText(path);
        return EntitySerializer.Deserialize(json, Provider.JsonSerializerOptions);
    }

    public bool EntityExists(string pk, string rk) => File.Exists(EntityPath(pk, rk));

    public void UpsertEntity(ITableEntity entity, TableUpdateMode mode)
    {
        var path = EntityPath(entity.PartitionKey, entity.RowKey);
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);

        if (mode == TableUpdateMode.Merge && File.Exists(path))
        {
            var existingJson = File.ReadAllText(path);
            var existing = EntitySerializer.Deserialize(existingJson, Provider.JsonSerializerOptions);
            var merged = EntitySerializer.MergeEntities(existing, entity);
            var etag = GenerateETag();
            var json = EntitySerializer.Serialize(merged, etag, Provider.JsonSerializerOptions);
            AtomicFile.WriteAllText(path, json);
        }
        else
        {
            var etag = GenerateETag();
            var json = EntitySerializer.Serialize(entity, etag, Provider.JsonSerializerOptions);
            AtomicFile.WriteAllText(path, json);
        }
    }

    public void UpdateEntity(ITableEntity entity, ETag ifMatch, TableUpdateMode mode)
    {
        var path = EntityPath(entity.PartitionKey, entity.RowKey);
        if (!File.Exists(path))
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);

        if (ifMatch != ETag.All)
        {
            var existingJson = File.ReadAllText(path);
            var existing = EntitySerializer.Deserialize(existingJson, Provider.JsonSerializerOptions);
            var existingETag = existing["odata.etag"]?.ToString();
            if (existingETag != ifMatch.ToString())
                throw new RequestFailedException(412, "ETag mismatch.", "UpdateConditionNotSatisfied", null);
        }

        UpsertEntity(entity, mode);
    }

    public void DeleteEntity(string pk, string rk, ETag ifMatch)
    {
        var path = EntityPath(pk, rk);
        if (!File.Exists(path))
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);

        if (ifMatch != ETag.All)
        {
            var json = File.ReadAllText(path);
            var existing = EntitySerializer.Deserialize(json, Provider.JsonSerializerOptions);
            var existingETag = existing["odata.etag"]?.ToString();
            if (existingETag != ifMatch.ToString())
                throw new RequestFailedException(412, "ETag mismatch.", "UpdateConditionNotSatisfied", null);
        }

        File.Delete(path);
    }

    public IEnumerable<TableEntity> EnumerateEntities()
    {
        if (!Directory.Exists(TablePath)) yield break;

        foreach (var pkDir in Directory.EnumerateDirectories(TablePath))
        {
            var dirName = Path.GetFileName(pkDir);
            if (dirName.StartsWith('.') || dirName.StartsWith('_')) continue;

            foreach (var file in Directory.EnumerateFiles(pkDir, "*.json"))
            {
                TableEntity? entity = null;
                try
                {
                    var json = File.ReadAllText(file);
                    entity = EntitySerializer.Deserialize(json, Provider.JsonSerializerOptions);
                }
                catch { /* skip corrupted files */ }
                if (entity is not null) yield return entity;
            }
        }
    }
}
