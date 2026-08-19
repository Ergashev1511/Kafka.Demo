using System.Diagnostics;
using Beepul.Afs.Kafka.Extensions;
using Confluent.Kafka;
using Kafka.Demo.MessageHandler;
using Kafka.Demo.Messages;
using Kafka.Demo.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddKafkaPublisher(builder.Configuration);
builder.Services.AddKafkaBatchConsumer<TransferEvent, TransferEventHandler>(builder.Configuration);
builder.Services.AddOptions<TransferLoadOptions>()
    .Bind(builder.Configuration.GetSection(TransferLoadOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.Topic), "LoadGenerator:Topic is required.")
    .Validate(options => options.TransfersPerSecond > 0, "LoadGenerator:TransfersPerSecond must be positive.")
    .Validate(options => options.MaxDurationSeconds > 0, "LoadGenerator:MaxDurationSeconds must be positive.")
    .ValidateOnStart();
builder.Services.AddSingleton<TransferLoadGenerator>();
builder.Services.AddSingleton<TransferPublisher>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapPost("/api/transfers/publish", async (
    TransferLoadGenerator generator,
    CancellationToken cancellationToken,
    int seconds = 1) =>
{
    try
    {
        var result = await generator.PublishAsync(seconds, cancellationToken);
        return Results.Ok(result);
    }
    catch (ArgumentOutOfRangeException exception)
    {
        return Results.BadRequest(new { error = exception.Message });
    }
    catch (InvalidOperationException exception)
    {
        return Results.Conflict(new { error = exception.Message });
    }
    catch (KafkaException exception)
    {
        return Results.Problem(
            title: "Kafka broker bilan bog‘lanib bo‘lmadi",
            detail: exception.Error.Reason,
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }
})
.WithName("PublishTransfers")
.WithOpenApi();

app.MapPost("/api/transfers", async (
    CreateTransferRequest request,
    TransferPublisher publisher,
    CancellationToken cancellationToken) =>
{
    try
    {
        var sw = Stopwatch.StartNew();
        var result = await publisher.PublishAsync(request, cancellationToken);
        sw.Stop();  
        //app.Logger.LogInformation("Transfer published in {Duration} ms", sw.ElapsedMilliseconds);
        return Results.Ok(result);
    }
    catch (KafkaException exception)
    {
        return Results.Problem(
            title: "Kafka broker bilan bog‘lanib bo‘lmadi",
            detail: exception.Error.Reason,
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }
})
.WithName("CreateTransfer")
.WithOpenApi();

app.Run();
