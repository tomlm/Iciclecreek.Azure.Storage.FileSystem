using Iciclecreek.Azure.Storage.Memory.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Blobs;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.Memory.Tests.Blobs;

public class BlobTagTests : BlobTagTestsBase
{
    protected override StorageTestFixture CreateFixture() => new MemoryStorageTestFixture();
}
