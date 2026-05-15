namespace Iciclecreek.Azure.Storage.SQLite.Tests;

/// <summary>
/// NUnit's metadata-based discovery mode skips assemblies that have no [Test] methods
/// defined locally (it doesn't scan inherited tests from referenced assemblies).
/// This class ensures the adapter loads and scans this assembly, which then discovers
/// all inherited tests from the shared base classes.
/// </summary>
[TestFixture]
public class AssemblyTestDiscoveryBootstrap
{
    [Test]
    public void Assembly_Is_Discoverable() => Assert.Pass();
}
