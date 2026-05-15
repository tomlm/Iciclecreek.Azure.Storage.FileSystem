using Iciclecreek.Azure.Storage.Memory.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Tables;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.Memory.Tests.Tables;

public class TableAdvancedFeatureTests : TableAdvancedFeatureTestsBase
{
    protected override StorageTestFixture CreateFixture() => new MemoryStorageTestFixture();
}
