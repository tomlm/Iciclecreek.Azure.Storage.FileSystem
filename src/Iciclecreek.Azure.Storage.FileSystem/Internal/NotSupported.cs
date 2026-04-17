using System.Runtime.CompilerServices;

namespace Iciclecreek.Azure.Storage.FileSystem.Internal;

internal static class NotSupported
{
    public static T Throw<T>([CallerMemberName] string memberName = "")
        => throw new NotSupportedException($"'{memberName}' is not supported by the file-backed client.");

    public static void Throw([CallerMemberName] string memberName = "")
        => throw new NotSupportedException($"'{memberName}' is not supported by the file-backed client.");

    public static T ThrowWithMessage<T>(string message)
        => throw new NotSupportedException(message);
}
