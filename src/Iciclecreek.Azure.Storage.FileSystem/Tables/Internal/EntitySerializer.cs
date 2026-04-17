using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Data.Tables;

namespace Iciclecreek.Azure.Storage.FileSystem.Tables.Internal;

internal static class EntitySerializer
{
    public static string Serialize(ITableEntity entity, string etag, JsonSerializerOptions options)
    {
        var doc = new EntityDocument
        {
            ETag = etag,
            PartitionKey = entity.PartitionKey,
            RowKey = entity.RowKey,
            Timestamp = entity.Timestamp ?? DateTimeOffset.UtcNow,
        };

        if (entity is TableEntity te)
        {
            foreach (var kvp in te)
            {
                if (kvp.Key is "PartitionKey" or "RowKey" or "odata.etag" or "Timestamp")
                    continue;
                doc.Properties[kvp.Key] = TypedValue.FromObject(kvp.Value);
            }
        }

        return JsonSerializer.Serialize(doc, options);
    }

    public static TableEntity Deserialize(string json, JsonSerializerOptions options)
    {
        var doc = JsonSerializer.Deserialize<EntityDocument>(json, options)!;
        var entity = new TableEntity(doc.PartitionKey, doc.RowKey)
        {
            Timestamp = doc.Timestamp,
        };
        entity["odata.etag"] = doc.ETag;

        foreach (var kvp in doc.Properties)
        {
            entity[kvp.Key] = kvp.Value.ToObject();
        }

        return entity;
    }

    public static TableEntity MergeEntities(TableEntity existing, ITableEntity incoming)
    {
        var merged = new TableEntity(existing.PartitionKey, existing.RowKey);
        foreach (var kvp in existing)
        {
            if (kvp.Key is "odata.etag" or "Timestamp") continue;
            merged[kvp.Key] = kvp.Value;
        }

        if (incoming is TableEntity te)
        {
            foreach (var kvp in te)
            {
                if (kvp.Key is "PartitionKey" or "RowKey" or "odata.etag" or "Timestamp") continue;
                if (kvp.Value is null)
                    merged.Remove(kvp.Key);
                else
                    merged[kvp.Key] = kvp.Value;
            }
        }
        return merged;
    }
}

internal sealed class EntityDocument
{
    [JsonPropertyName("$etag")]
    public string ETag { get; set; } = "";

    [JsonPropertyName("$pk")]
    public string PartitionKey { get; set; } = "";

    [JsonPropertyName("$rk")]
    public string RowKey { get; set; } = "";

    [JsonPropertyName("$timestamp")]
    public DateTimeOffset Timestamp { get; set; }

    [JsonPropertyName("Properties")]
    public Dictionary<string, TypedValue> Properties { get; set; } = new();
}

internal sealed class TypedValue
{
    [JsonPropertyName("type")]
    public string Type { get; set; } = "String";

    [JsonPropertyName("value")]
    public string Value { get; set; } = "";

    public static TypedValue FromObject(object? value)
    {
        if (value is null) return new TypedValue { Type = "Null", Value = "" };
        return value switch
        {
            string s => new TypedValue { Type = "String", Value = s },
            int i => new TypedValue { Type = "Int32", Value = i.ToString() },
            long l => new TypedValue { Type = "Int64", Value = l.ToString() },
            double d => new TypedValue { Type = "Double", Value = d.ToString("R") },
            bool b => new TypedValue { Type = "Boolean", Value = b.ToString() },
            DateTimeOffset dto => new TypedValue { Type = "DateTime", Value = dto.UtcDateTime.ToString("O") },
            DateTime dt => new TypedValue { Type = "DateTime", Value = dt.ToUniversalTime().ToString("O") },
            Guid g => new TypedValue { Type = "Guid", Value = g.ToString() },
            byte[] bytes => new TypedValue { Type = "Binary", Value = Convert.ToBase64String(bytes) },
            BinaryData bd => new TypedValue { Type = "Binary", Value = Convert.ToBase64String(bd.ToArray()) },
            _ => new TypedValue { Type = "String", Value = value.ToString() ?? "" },
        };
    }

    public object? ToObject() => Type switch
    {
        "String" => Value,
        "Int32" => int.Parse(Value),
        "Int64" => long.Parse(Value),
        "Double" => double.Parse(Value),
        "Boolean" => bool.Parse(Value),
        "DateTime" => DateTimeOffset.Parse(Value),
        "Guid" => Guid.Parse(Value),
        "Binary" => Convert.FromBase64String(Value),
        "Null" => null,
        _ => Value,
    };
}
