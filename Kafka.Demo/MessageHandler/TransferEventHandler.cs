using Beepul.Afs.Kafka.Abstractions;
using Kafka.Demo.Messages;

namespace Kafka.Demo.MessageHandler;

public sealed class TransferEventHandler(
    ILogger<TransferEventHandler> logger) : KafkaEventHandler<TransferEvent>
{
    public override Task HandleAsync(
        IReadOnlyList<KafkaEvent<TransferEvent>> batch,
        CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "Batch qabul qilindi va skip qilindi. EventCount={EventCount}, TransferCount={TransferCount}",
            batch.Count,
            batch.Select(item => item.Payload.Transfer.TransferId).Distinct().Count());

        return Task.CompletedTask;
    }
}
