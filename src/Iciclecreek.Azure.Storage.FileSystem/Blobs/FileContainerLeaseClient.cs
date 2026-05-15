using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs;

/// <summary>
/// Filesystem-backed lease client for containers.
/// Lease state is stored in <c>{container}/_container.lease.json</c>.
/// </summary>
public class FileContainerLeaseClient : BlobLeaseClient
{
    private readonly FileBlobContainerClient _container;
    private readonly string _leasePath;
    private string _leaseId;

    internal FileContainerLeaseClient(FileBlobContainerClient container, string? leaseId) : base()
    {
        _container = container;
        _leaseId = leaseId ?? Guid.NewGuid().ToString();
        _leasePath = Path.Combine(container.ContainerPath, "_container.lease.json");
    }

    public override string LeaseId => _leaseId;
    public new Uri Uri => _container.Uri;

    // ── Acquire ─────────────────────────────────────────────────────────

    public override async Task<Response<BlobLease>> AcquireAsync(TimeSpan duration, RequestConditions? conditions = null, CancellationToken ct = default)
    {
        var state = await ReadLeaseStateAsync(ct).ConfigureAwait(false);
        if (state.IsActive)
            throw new RequestFailedException(409, "There is already a lease present.", "LeaseAlreadyPresent", null);

        var secs = (int)duration.TotalSeconds;
        if (secs != -1 && (secs < 15 || secs > 60))
            throw new RequestFailedException(400, "Lease duration must be 15-60 seconds or -1 (infinite).", "InvalidHeaderValue", null);

        state.LeaseId = _leaseId;
        state.DurationSeconds = secs;
        state.ExpiresOn = secs == -1 ? null : DateTimeOffset.UtcNow.AddSeconds(secs);
        await WriteLeaseStateAsync(state, ct).ConfigureAwait(false);

        return Response.FromValue(BlobsModelFactory.BlobLease(ETag.All, DateTimeOffset.UtcNow, _leaseId), StubResponse.Created());
    }

    public override Response<BlobLease> Acquire(TimeSpan duration, RequestConditions? conditions = null, CancellationToken ct = default)
        => AcquireAsync(duration, conditions, ct).GetAwaiter().GetResult();

    // ── Renew ───────────────────────────────────────────────────────────

    public override async Task<Response<BlobLease>> RenewAsync(RequestConditions? conditions = null, CancellationToken ct = default)
    {
        var state = await ReadLeaseStateAsync(ct).ConfigureAwait(false);
        if (state.LeaseId != _leaseId)
            throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

        if (state.DurationSeconds != -1)
            state.ExpiresOn = DateTimeOffset.UtcNow.AddSeconds(state.DurationSeconds);
        await WriteLeaseStateAsync(state, ct).ConfigureAwait(false);

        return Response.FromValue(BlobsModelFactory.BlobLease(ETag.All, DateTimeOffset.UtcNow, _leaseId), StubResponse.Ok());
    }

    public override Response<BlobLease> Renew(RequestConditions? conditions = null, CancellationToken ct = default)
        => RenewAsync(conditions, ct).GetAwaiter().GetResult();

    // ── Release ─────────────────────────────────────────────────────────

    public override async Task<Response<ReleasedObjectInfo>> ReleaseAsync(RequestConditions? conditions = null, CancellationToken ct = default)
    {
        var state = await ReadLeaseStateAsync(ct).ConfigureAwait(false);
        if (state.LeaseId != _leaseId)
            throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

        state.LeaseId = null;
        state.ExpiresOn = null;
        state.DurationSeconds = 0;
        await WriteLeaseStateAsync(state, ct).ConfigureAwait(false);

        return Response.FromValue(new ReleasedObjectInfo(ETag.All, DateTimeOffset.UtcNow), StubResponse.Ok());
    }

    public override Response<ReleasedObjectInfo> Release(RequestConditions? conditions = null, CancellationToken ct = default)
        => ReleaseAsync(conditions, ct).GetAwaiter().GetResult();

    // ── Change ──────────────────────────────────────────────────────────

    public override async Task<Response<BlobLease>> ChangeAsync(string proposedId, RequestConditions? conditions = null, CancellationToken ct = default)
    {
        var state = await ReadLeaseStateAsync(ct).ConfigureAwait(false);
        if (state.LeaseId != _leaseId)
            throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

        state.LeaseId = proposedId;
        await WriteLeaseStateAsync(state, ct).ConfigureAwait(false);
        _leaseId = proposedId;

        return Response.FromValue(BlobsModelFactory.BlobLease(ETag.All, DateTimeOffset.UtcNow, proposedId), StubResponse.Ok());
    }

    public override Response<BlobLease> Change(string proposedId, RequestConditions? conditions = null, CancellationToken ct = default)
        => ChangeAsync(proposedId, conditions, ct).GetAwaiter().GetResult();

    // ── Break ───────────────────────────────────────────────────────────

    public override async Task<Response<BlobLease>> BreakAsync(TimeSpan? breakPeriod = null, RequestConditions? conditions = null, CancellationToken ct = default)
    {
        var state = await ReadLeaseStateAsync(ct).ConfigureAwait(false);
        if (!state.IsActive)
            throw new RequestFailedException(409, "There is currently no lease on the container.", "LeaseNotPresentWithLeaseOperation", null);

        var oldId = state.LeaseId;
        state.LeaseId = null;
        state.ExpiresOn = null;
        state.DurationSeconds = 0;
        await WriteLeaseStateAsync(state, ct).ConfigureAwait(false);

        return Response.FromValue(BlobsModelFactory.BlobLease(ETag.All, DateTimeOffset.UtcNow, oldId), StubResponse.Ok());
    }

    public override Response<BlobLease> Break(TimeSpan? breakPeriod = null, RequestConditions? conditions = null, CancellationToken ct = default)
        => BreakAsync(breakPeriod, conditions, ct).GetAwaiter().GetResult();

    // ── State persistence ───────────────────────────────────────────────

    private async Task<ContainerLeaseState> ReadLeaseStateAsync(CancellationToken ct)
    {
        if (!File.Exists(_leasePath)) return new ContainerLeaseState();
        var json = await File.ReadAllTextAsync(_leasePath, ct).ConfigureAwait(false);
        return JsonSerializer.Deserialize<ContainerLeaseState>(json) ?? new ContainerLeaseState();
    }

    private async Task WriteLeaseStateAsync(ContainerLeaseState state, CancellationToken ct)
    {
        var json = JsonSerializer.Serialize(state);
        await AtomicFile.WriteAllTextAsync(_leasePath, json, ct).ConfigureAwait(false);
    }

    private sealed class ContainerLeaseState
    {
        public string? LeaseId { get; set; }
        public DateTimeOffset? ExpiresOn { get; set; }
        public int DurationSeconds { get; set; }

        [System.Text.Json.Serialization.JsonIgnore]
        public bool IsActive =>
            LeaseId != null &&
            (DurationSeconds == -1 || (ExpiresOn.HasValue && ExpiresOn.Value > DateTimeOffset.UtcNow));
    }
}
