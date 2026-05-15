using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Blobs;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class AppendBlobTests : AppendBlobTestsBase
{
    protected override StorageTestFixture CreateFixture() => new FileStorageTestFixture();
}
