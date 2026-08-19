using Beepul.Afs.Kafka.Abstractions;
using Kafka.Demo.Messages;
using Kafka.Demo.Models;
using Microsoft.Extensions.Options;

namespace Kafka.Demo.Services;

public sealed class TransferPublisher(
    IEventPublisher publisher,
    IOptions<TransferLoadOptions> options)
{
    private readonly string _topic = options.Value.Topic;

    public async Task<TransferPublishResult> PublishAsync(
        CreateTransferRequest request,
        CancellationToken cancellationToken)
    {
        var transfer = new Transfer(
            TransferId: Guid.NewGuid(),
            SenderAccount: request.SenderAccount,
            ReceiverAccount: request.ReceiverAccount,
            Amount: request.Amount,
            Currency: request.Currency,
            CreatedAt: DateTimeOffset.UtcNow);

        KafkaPublishRequest<TransferEvent>[] events =
        [
            CreateEvent(transfer, TransferStep.Created, "transfer.created"),
            CreateEvent(transfer, TransferStep.Authorized, "transfer.authorized"),
            CreateEvent(transfer, TransferStep.Paid, "transfer.paid")
        ];

        await publisher.PublishBatchAsync(events, cancellationToken);

        return new TransferPublishResult(transfer.TransferId, events.Length);
    }

    private KafkaPublishRequest<TransferEvent> CreateEvent(
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

        return new KafkaPublishRequest<TransferEvent>(
            _topic,
            envelope,
            key: $"transfer:{transfer.TransferId:N}");
    }
}

public sealed record CreateTransferRequest(
    string SenderAccount,
    string ReceiverAccount,
    decimal Amount,
    string Currency);

public sealed record TransferPublishResult(
    Guid TransferId,
    int EventsPublished);
