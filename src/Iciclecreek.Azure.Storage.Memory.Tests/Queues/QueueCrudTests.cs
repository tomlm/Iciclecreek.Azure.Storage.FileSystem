using Iciclecreek.Azure.Storage.Memory.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Queues;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.Memory.Tests.Queues;

public class QueueCrudTests : QueueCrudTestsBase
{
    protected override StorageTestFixture CreateFixture() => new MemoryStorageTestFixture();
}
