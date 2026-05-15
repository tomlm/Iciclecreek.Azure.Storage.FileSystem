![icon](icon.png)
# Iciclecreek.Azure.Storage

**Drop-in replacements** for Azure Storage SDK clients. Use the same `BlobContainerClient`, `TableClient`, and `QueueClient` types in tests and local development without Azurite or a live Azure account.

Choose the backend that fits your scenario: in-memory for fast unit tests, filesystem for inspectable persistent state, SQLite for single-file portable storage, or the REST server for integration testing.

## Packages

| Package | Description | NuGet |
|---------|-------------|-------|
| [Iciclecreek.Azure.Storage.Memory](src/Libraries/Iciclecreek.Azure.Storage.Memory) | Thread-safe in-memory blobs, tables, and queues | [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.Memory.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Memory) |
| [Iciclecreek.Azure.Storage.FileSystem](src/Libraries/Iciclecreek.Azure.Storage.FileSystem) | Filesystem-backed blobs, tables, and queues | [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.FileSystem.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.FileSystem) |
| [Iciclecreek.Azure.Storage.SQLite](src/Libraries/Iciclecreek.Azure.Storage.SQLite) | SQLite-backed blobs, tables, and queues | [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.SQLite.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.SQLite) |
| [Iciclecreek.Azure.Storage.Server](src/Libraries/Iciclecreek.Azure.Storage.Server) | ASP.NET Core controllers exposing the Azure Storage REST API | [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.Server.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Server) |

## Supported Features

| Area | Memory | FileSystem | SQLite |
|------|:------:|:----------:|:------:|
| Block blobs (Upload, Download, Stage, Commit) | Y | Y | Y |
| Append blobs (Create, Append) | Y | Y | Y |
| Page blobs (Create, UploadPages, ClearPages) | Y | Y | Y |
| Blob containers (CRUD, Listing, Hierarchy) | Y | Y | Y |
| Blob metadata, HTTP headers, tags | Y | Y | Y |
| ETag concurrency (IfMatch / IfNoneMatch) | Y | Y | Y |
| Blob leases and container leases | Y | Y | Y |
| Blob copy (StartCopyFromUri, SyncCopyFromUri) | Y | Y | Y |
| Blob OpenRead / OpenWrite streaming | Y | Y | Y |
| Blob snapshots and versions | Y | Y | Y |
| Tables (CRUD, Upsert, Merge, Replace) | Y | Y | Y |
| Table OData queries and LINQ expressions | Y | Y | Y |
| Table transactions with rollback | Y | Y | Y |
| Queues (CRUD, Send, Receive, Peek, Delete) | Y | Y | Y |
| Queue visibility timeout and TTL | Y | Y | Y |

## Quick Start

```csharp
// Memory provider -- fastest, no I/O, ideal for unit tests
var provider = new MemoryStorageProvider();
var account  = provider.AddAccount("devaccount");

// FileSystem provider -- stores data as files on disk
var provider = new FileStorageProvider(@"C:\temp\my-storage");
var account  = provider.AddAccount("devaccount");

// SQLite provider -- stores data in a .db file per account
var provider = new SqliteStorageProvider(@"C:\temp\my-storage");
var account  = provider.AddAccount("devaccount");

// Then use standard Azure SDK types with any provider
BlobContainerClient container = MemoryBlobContainerClient.FromAccount(account, "my-container");
```

See each package's README for detailed usage.

## License

MIT
