namespace Kafka.Demo.Services;

public sealed class TransferLoadOptions
{
    public const string SectionName = "LoadGenerator";

    public string Topic { get; set; } = string.Empty;
    public int TransfersPerSecond { get; set; }
    public int MaxDurationSeconds { get; set; }
}
