![icon](../../icon.png)

[![Build](https://github.com/tomlm/Iciclecreek.Azure.Storage/actions/workflows/BuildAndRunTests.yml/badge.svg)](https://github.com/tomlm/Iciclecreek.Azure.Storage/actions/workflows/BuildAndRunTests.yml) [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.FileSystem.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.FileSystem)

# Iciclecreek.Azure.Storage.FileSystem

A **filesystem-backed drop-in replacement** for `Azure.Storage.Blobs`, `Azure.Data.Tables`, and `Azure.Storage.Queues` clients. Use the same Azure SDK types in tests and local development without Azurite or a live Azure account.

State is stored as real files on disk -- human-inspectable and survives process restarts.

## Installation

```
dotnet add package Iciclecreek.Azure.Storage.FileSystem
```

## Usage

### Create a provider and account

```csharp
using Iciclecreek.Azure.Storage.FileSystem;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Queues;

var provider = new FileStorageProvider(@"C:\temp\my-storage");
var account  = provider.AddAccount("devaccount");
```

### Blobs

```csharp
BlobContainerClient container = FileBlobContainerClient.FromAccount(account, "my-container");
await container.CreateIfNotExistsAsync();

BlobClient blob = container.GetBlobClient("hello.txt");
await blob.UploadAsync(BinaryData.FromString("Hello, World!"));

var result = (await blob.DownloadContentAsync()).Value;
Console.WriteLine(result.Content.ToString()); // "Hello, World!"
```

### Tables

```csharp
TableClient table = FileTableClient.FromAccount(account, "people");
await table.CreateIfNotExistsAsync();

await table.AddEntityAsync(new TableEntity("users", "alice") { ["Name"] = "Alice" });
var entity = (await table.GetEntityAsync<TableEntity>("users", "alice")).Value;
```

### Queues

```csharp
QueueClient queue = FileQueueClient.FromAccount(account, "tasks");
queue.Create();

queue.SendMessage("do the thing");
var msg = queue.ReceiveMessage().Value;
Console.WriteLine(msg.Body.ToString()); // "do the thing"
```

### Swap in via dependency injection

Every `File*` client inherits from its Azure SDK base type:

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

## Related Packages

| Package | Description |
|---------|-------------|
| [Iciclecreek.Azure.Storage.Memory](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Memory) | Thread-safe in-memory implementation (fastest, no I/O) |
| [Iciclecreek.Azure.Storage.SQLite](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.SQLite) | SQLite-backed implementation (single .db file per account) |
| [Iciclecreek.Azure.Storage.Server](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Server) | ASP.NET Core REST API server on top of any provider |

## License

MIT
