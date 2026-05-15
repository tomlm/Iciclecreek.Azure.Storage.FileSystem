using Iciclecreek.Azure.Storage.Memory.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Blobs;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.Memory.Tests.Blobs;

public class BlockBlobTests : BlockBlobTestsBase
{
    protected override StorageTestFixture CreateFixture() => new MemoryStorageTestFixture();
}
