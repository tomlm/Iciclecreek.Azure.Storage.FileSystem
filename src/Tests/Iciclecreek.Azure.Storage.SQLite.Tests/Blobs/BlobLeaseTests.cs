using Iciclecreek.Azure.Storage.SQLite.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Blobs;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.SQLite.Tests.Blobs;

public class BlobLeaseTests : BlobLeaseTestsBase
{
    protected override StorageTestFixture CreateFixture() => new SqliteStorageTestFixture();
}
