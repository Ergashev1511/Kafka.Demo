using Kafka.Demo.Models;

namespace Kafka.Demo.Messages;

public sealed record TransferEvent(
    Transfer Transfer,
    TransferStep Step,
    int Sequence);

public enum TransferStep
{
    Created = 1,
    Authorized = 2,
    Paid = 3
}
