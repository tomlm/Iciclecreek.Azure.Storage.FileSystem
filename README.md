# Iciclecreek.Azure.Storage.FileSystem

A **filesystem-backed drop-in replacement** for `Azure.Storage.Blobs` and `Azure.Data.Tables` clients. Use the same Azure SDK types in your 
tests and local development without running Azurite or connecting to a live Azure account.

Inspired by [Spotflow.InMemory.Azure.Storage](https://github.com/spotflow-io/in-memory-azure-test-sdk), but backed by the real filesystem -- 
state survives process restarts and is human-inspectable on disk.

## Features

| Area | Supported |
|---|---|
| **Block blobs** | Upload, Download, StageBlock, CommitBlockList, GetBlockList |
| **Append blobs** | Create, AppendBlock |
| **Blob containers** | Create, Delete, Exists, GetBlobs, GetBlobsByHierarchy |
| **Blob metadata** | SetMetadata, SetHttpHeaders, ContentType, ETag conditions (IfMatch / IfNoneMatch) |
| **Tables** | AddEntity, GetEntity, UpsertEntity (Merge + Replace), UpdateEntity, DeleteEntity |
| **Table queries** | OData string filters (`eq`, `ne`, `gt`, `ge`, `lt`, `le`, `and`, `or`, `not`) and LINQ expressions |
| **Transactions** | SubmitTransaction with automatic rollback on failure |
| **ETag concurrency** | Full optimistic concurrency on both blobs and table entities |
| **Async I/O** | All disk operations are truly async (`FileStream` with `useAsync: true`) |

## Install

```
dotnet add package Iciclecreek.Azure.Storage.FileSystem
```

**Package dependencies:** `Azure.Storage.Blobs` (>= 12.27.0), `Azure.Data.Tables` (>= 12.11.0)

## Quick Start

### 1. Create a provider and account

```csharp
using Iciclecreek.Azure.Storage.FileSystem;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tables;

// Point at any folder -- it will be created if it doesn't exist.
var provider = new FileStorageProvider(@"C:\temp\my-storage");
var account  = provider.AddAccount("devaccount");
```

### 2. Use blob clients exactly like the real Azure SDK

```csharp
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

// Get a container client (same base type as the real SDK).
BlobContainerClient container = FileBlobContainerClient.FromAccount(account, "my-container");
await container.CreateIfNotExistsAsync();

// Upload
BlobClient blob = container.GetBlobClient("hello.txt");
await blob.UploadAsync(BinaryData.FromString("Hello, World!"));

// Download
BlobDownloadResult result = (await blob.DownloadContentAsync()).Value;
Console.WriteLine(result.Content.ToString()); // "Hello, World!"
```

### 3. Use table clients the same way

```csharp
using Azure.Data.Tables;

TableClient table = FileTableClient.FromAccount(account, "people");
await table.CreateIfNotExistsAsync();

await table.AddEntityAsync(new TableEntity("users", "alice") { ["Name"] = "Alice" });

var entity = (await table.GetEntityAsync<TableEntity>("users", "alice")).Value;
Console.WriteLine(entity["Name"]); // "Alice"
```

### 4. Swap in tests via dependency injection

Because every `File*` client inherits from its Azure SDK base type, you can substitute them directly:

```csharp
// Production
services.AddSingleton<BlobContainerClient>(
    new BlobContainerClient(connectionString, "images"));

// Test
var provider = new FileStorageProvider(testDir);
var account  = provider.AddAccount("test");
services.AddSingleton<BlobContainerClient>(
    FileBlobContainerClient.FromAccount(account, "images"));
```

The rest of your code sees `BlobContainerClient` / `TableClient` and doesn't know the difference.

## Details

### On-Disk Layout

```
<root>/
  <account>/
    blobs/
      <container>/
        path/to/blob              # raw blob bytes
        path/to/blob.meta.json    # sidecar: ETag, content-type, metadata, block list
        .blocks/                  # uncommitted block staging area
    tables/
      <table>/
        <partitionKey>/
          <rowKey>.json           # entity JSON with EDM type preservation
```

Blobs are stored as real files -- you can open them in any editor. Sidecars (`.meta.json`) hold content-type, ETag, user metadata, and committed block lists. Table entities are one JSON file per entity with typed property preservation (`Int64`, `DateTime`, `Guid`, `Binary`).

### Client Construction

Three ways to create any client:

```csharp
// 1. From account (simplest)
var container = FileBlobContainerClient.FromAccount(account, "my-container");
var table     = FileTableClient.FromAccount(account, "my-table");

// 2. From connection string (matches real Azure SDK pattern)
var container = new FileBlobContainerClient(account.GetConnectionString(), "my-container", provider);
var table     = new FileTableClient(account.GetConnectionString(), "my-table", provider);

// 3. From URI
var container = new FileBlobContainerClient(new Uri("https://devaccount.blob.storage.file.local/my-container"), provider);
```

### Specialized Blob Clients

```csharp
// Block blobs -- stage and commit
FileBlockBlobClient blockBlob = container.GetFileBlockBlobClient("large-file.bin");
await blockBlob.StageBlockAsync(blockId1, stream1);
await blockBlob.StageBlockAsync(blockId2, stream2);
await blockBlob.CommitBlockListAsync(new[] { blockId1, blockId2 });

// Append blobs -- create and append
FileAppendBlobClient appendBlob = container.GetFileAppendBlobClient("log.txt");
await appendBlob.CreateAsync(new AppendBlobCreateOptions());
await appendBlob.AppendBlockAsync(new MemoryStream("log line\n"u8.ToArray()));
```

### ETag / Optimistic Concurrency

ETags are computed on every write and stored in the sidecar. Both `IfMatch` and `IfNoneMatch` conditions are supported:

```csharp
// Prevent overwrite of existing blob
await blob.UploadAsync(data, overwrite: false); // uses IfNoneMatch = *

// Read-modify-write with ETag guard
var props = (await blob.GetPropertiesAsync()).Value;
await blob.UploadAsync(newData, new BlobUploadOptions
{
    Conditions = new BlobRequestConditions { IfMatch = props.ETag }
});

// Table entity optimistic concurrency
var entity = (await table.GetEntityAsync<TableEntity>("pk", "rk")).Value;
await table.UpdateEntityAsync(updatedEntity, entity.ETag, TableUpdateMode.Replace);
```

### Table Queries

String filters and LINQ expressions are both supported:

```csharp
// OData string filter
await foreach (var e in table.QueryAsync<TableEntity>("PartitionKey eq 'users' and Age gt 25"))
    Console.WriteLine(e["Name"]);

// LINQ expression
await foreach (var e in table.QueryAsync<TableEntity>(e => e.PartitionKey == "users"))
    Console.WriteLine(e["Name"]);
```

Supported filter operators: `eq`, `ne`, `gt`, `ge`, `lt`, `le`, `and`, `or`, `not`, with string, int, long, double, bool, `datetime'...'`, and `guid'...'` literals.

### Transactions

```csharp
var actions = new[]
{
    new TableTransactionAction(TableTransactionActionType.Add, entity1),
    new TableTransactionAction(TableTransactionActionType.Add, entity2),
};
await table.SubmitTransactionAsync(actions);
```

All entities must share the same `PartitionKey`. If any action fails, all preceding changes are rolled back.

### Provider Rehydration

When you create a new `FileStorageProvider` pointing at an existing directory, it discovers previously created accounts automatically:

```csharp
var provider = new FileStorageProvider(@"C:\temp\my-storage");
// Any accounts created in prior runs are available immediately.
var account = provider.GetAccount("devaccount");
```

### Unsupported Operations

Methods not listed above will throw `NotSupportedException` when called. This matches the behavior of [Spotflow's in-memory SDK](https://github.com/spotflow-io/in-memory-azure-test-sdk). 
Notable exclusions: leases, snapshots, blob copy, access tiers, SAS signature validation, blob tags, immutability policies.

### Thread Safety

Clients are safe for concurrent use within a single process. File locking uses `FileShare.None` with retry, which is mandatory on Windows and advisory on Linux.
This library is designed as a **test fake** -- not a production storage engine.

## License

MIT
