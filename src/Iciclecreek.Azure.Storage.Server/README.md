# Iciclecreek.Azure.Storage.Server

ASP.NET Core controllers that expose the **Azure Storage REST API** on top of any `BlobServiceClient`, `TableServiceClient`, and `QueueServiceClient` implementation. Pair with the FileSystem or SQLite provider to run a local Azure Storage-compatible server.

## Installation

```
dotnet add package Iciclecreek.Azure.Storage.Server
```

## Usage

### Register the controllers

```csharp
using Iciclecreek.Azure.Storage.Server;
using Iciclecreek.Azure.Storage.FileSystem;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Queues;

var builder = WebApplication.CreateBuilder(args);

// Set up the storage provider
var provider = new FileStorageProvider(@"C:\temp\my-storage");
var account  = provider.AddAccount("devaccount");

// Register Azure SDK clients in DI
builder.Services.AddSingleton(FileBlobServiceClient.FromAccount(account));
builder.Services.AddSingleton(FileTableServiceClient.FromAccount(account));
builder.Services.AddSingleton(FileQueueServiceClient.FromAccount(account));

// Add the storage server controllers
builder.Services.AddStorageServer();

var app = builder.Build();
app.MapStorageServer();
app.Run();
```

### Use with any provider

The server works with any Azure SDK client implementation -- FileSystem, SQLite, or even the real Azure clients:

```csharp
// SQLite provider
var provider = new SqliteStorageProvider(@"C:\temp\my-storage");
var account  = provider.AddAccount("devaccount");
builder.Services.AddSingleton(SqliteBlobServiceClient.FromAccount(account));
builder.Services.AddSingleton(SqliteTableServiceClient.FromAccount(account));
builder.Services.AddSingleton(SqliteQueueServiceClient.FromAccount(account));
```

## Related Packages

| Package | Description |
|---------|-------------|
| [Iciclecreek.Azure.Storage.Memory](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Memory) | Thread-safe in-memory blobs, tables, and queues |
| [Iciclecreek.Azure.Storage.FileSystem](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.FileSystem) | Filesystem-backed blobs, tables, and queues |
| [Iciclecreek.Azure.Storage.SQLite](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.SQLite) | SQLite-backed blobs, tables, and queues |

## License

MIT
