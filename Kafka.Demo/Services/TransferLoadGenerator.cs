using System.Diagnostics;
using Beepul.Afs.Kafka.Abstractions;
using Kafka.Demo.Messages;
using Kafka.Demo.Models;
using Microsoft.Extensions.Options;

namespace Kafka.Demo.Services;

public sealed class TransferLoadGenerator(
    IEventPublisher publisher,
    IOptions<TransferLoadOptions> options,
    ILogger<TransferLoadGenerator> logger)
{
    private readonly TransferLoadOptions _options = options.Value;
    private readonly SemaphoreSlim _executionLock = new(1, 1);

    public async Task<PublishResult> PublishAsync(
        int seconds,
        CancellationToken cancellationToken)
    {
        if (seconds < 1 || seconds > _options.MaxDurationSeconds)
            throw new ArgumentOutOfRangeException(
                nameof(seconds),
                $"Seconds must be between 1 and {_options.MaxDurationSeconds}.");

        if (!await _executionLock.WaitAsync(0, cancellationToken))
            throw new InvalidOperationException("A transfer publishing job is already running.");

        var totalTransfers = 0;
        var totalEvents = 0;
        var totalDuration = Stopwatch.StartNew();

        try
        {
            for (var second = 0; second < seconds; second++)
            {
                var interval = Stopwatch.StartNew();
                var requests = CreateSecondBatch();

                await publisher.PublishBatchAsync(requests, cancellationToken);

                totalTransfers += _options.TransfersPerSecond;
                totalEvents += requests.Count;

                var remaining = TimeSpan.FromSeconds(1) - interval.Elapsed;
                if (remaining > TimeSpan.Zero && second < seconds - 1)
                    await Task.Delay(remaining, cancellationToken);
            }

            logger.LogInformation(
                "Load generation completed. Transfers={Transfers}, Events={Events}, DurationMs={DurationMs}",
                totalTransfers,
                totalEvents,
                totalDuration.ElapsedMilliseconds);

            return new PublishResult(totalTransfers, totalEvents, totalDuration.Elapsed);
        }
        finally
        {
            _executionLock.Release();
        }
    }

    private IReadOnlyCollection<KafkaPublishRequest<TransferEvent>> CreateSecondBatch()
    {
        var requests = new List<KafkaPublishRequest<TransferEvent>>(_options.TransfersPerSecond * 3);

        for (var index = 0; index < _options.TransfersPerSecond; index++)
        {
            var transfer = CreateTransfer(index);
            AddEvent(requests, transfer, TransferStep.Created, "transfer.created");
            AddEvent(requests, transfer, TransferStep.Authorized, "transfer.authorized");
            AddEvent(requests, transfer, TransferStep.Paid, "transfer.paid");
        }

        return requests;
    }

    private static Transfer CreateTransfer(int index)
    {
        var transferId = Guid.NewGuid();
        return new Transfer(
            TransferId: transferId,
            SenderAccount: $"8600{index:D12}",
            ReceiverAccount: $"9860{index:D12}",
            Amount: 10_000m + index,
            Currency: "UZS",
            CreatedAt: DateTimeOffset.UtcNow);
    }

    private void AddEvent(
        ICollection<KafkaPublishRequest<TransferEvent>> requests,
        Transfer transfer,
        TransferStep step,
        string eventType)
    {
        var payload = new TransferEvent(transfer, step, (int)step);
        var envelope = new KafkaEvent<TransferEvent>(
            eventId: Guid.NewGuid(),
            eventType: eventType,
            occurredAt: DateTimeOffset.UtcNow,
            payload: payload);

        requests.Add(new KafkaPublishRequest<TransferEvent>(
            _options.Topic,
            envelope,
            key: $"transfer:{transfer.TransferId:N}"));
    }
}

public sealed record PublishResult(
    int TransfersPublished,
    int EventsPublished,
    TimeSpan Duration);
