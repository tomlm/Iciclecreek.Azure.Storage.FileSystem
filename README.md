# Iciclecreek.Azure.Storage

**Drop-in replacements** for Azure Storage SDK clients backed by local storage. Use the same `BlobContainerClient`, `TableClient`, and `QueueClient` types in tests and local development without Azurite or a live Azure account.

Inspired by [Spotflow.InMemory.Azure.Storage](https://github.com/spotflow-io/in-memory-azure-test-sdk), but backed by real persistent storage -- state survives process restarts and is human-inspectable.

## Packages

| Package | Description | NuGet |
|---------|-------------|-------|
| [Iciclecreek.Azure.Storage.FileSystem](src/Iciclecreek.Azure.Storage.FileSystem) | Filesystem-backed blobs, tables, and queues | [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.FileSystem.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.FileSystem) |
| [Iciclecreek.Azure.Storage.SQLite](src/Iciclecreek.Azure.Storage.SQLite) | SQLite-backed blobs, tables, and queues | [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.SQLite.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.SQLite) |
| [Iciclecreek.Azure.Storage.Server](src/Iciclecreek.Azure.Storage.Server) | ASP.NET Core controllers exposing the Azure Storage REST API | [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.Server.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Server) |

## Supported Features

| Area | FileSystem | SQLite |
|------|:----------:|:------:|
| Block blobs (Upload, Download, Stage, Commit) | Y | Y |
| Append blobs (Create, Append) | Y | Y |
| Page blobs (Create, UploadPages, ClearPages) | Y | Y |
| Blob containers (CRUD, Listing, Hierarchy) | Y | Y |
| Blob metadata, HTTP headers, tags | Y | Y |
| ETag concurrency (IfMatch / IfNoneMatch) | Y | Y |
| Blob leases and container leases | Y | Y |
| Blob copy (StartCopyFromUri, SyncCopyFromUri) | Y | Y |
| Blob OpenRead / OpenWrite streaming | Y | Y |
| Blob snapshots and versions | Y | Y |
| Tables (CRUD, Upsert, Merge, Replace) | Y | Y |
| Table OData queries and LINQ expressions | Y | Y |
| Table transactions with rollback | Y | Y |
| Queues (CRUD, Send, Receive, Peek, Delete) | Y | Y |
| Queue visibility timeout and TTL | Y | Y |

## Quick Start

```csharp
// FileSystem provider -- stores data as files on disk
var provider = new FileStorageProvider(@"C:\temp\my-storage");
var account  = provider.AddAccount("devaccount");

// SQLite provider -- stores data in a .db file per account
var provider = new SqliteStorageProvider(@"C:\temp\my-storage");
var account  = provider.AddAccount("devaccount");

// Then use standard Azure SDK types
BlobContainerClient container = FileBlobContainerClient.FromAccount(account, "my-container");
// or
BlobContainerClient container = SqliteBlobContainerClient.FromAccount(account, "my-container");
```

See each package's README for detailed usage.

## License

MIT
