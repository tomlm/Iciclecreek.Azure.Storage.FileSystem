using Iciclecreek.Azure.Storage.SQLite.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Tables;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.SQLite.Tests.Tables;

public class TableTransactionTests : TableTransactionTestsBase
{
    protected override StorageTestFixture CreateFixture() => new SqliteStorageTestFixture();
}
