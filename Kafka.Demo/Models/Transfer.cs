namespace Kafka.Demo.Models;

public sealed record Transfer(
    Guid TransferId,
    string SenderAccount,
    string ReceiverAccount,
    decimal Amount,
    string Currency,
    DateTimeOffset CreatedAt);
