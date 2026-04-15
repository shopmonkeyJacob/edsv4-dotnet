using EDS.Core.Abstractions;
using Spectre.Console;
using ValidationResult = Spectre.Console.ValidationResult;

namespace EDS.Cli;

/// <summary>
/// Interactive wizard that prompts the user to configure server tuning options
/// and writes the chosen values to config.toml. Runs once after fresh enrollment.
/// </summary>
internal static class ConfigWizard
{
    internal sealed record Result(
        DriverMode Mode,
        string     EventsSchema,
        int        MinPendingLatencySecs,
        int        MaxPendingLatencySecs,
        int        MaxAckPending);

    private const string TimeSeriesChoice =
        "timeseries — append every event to an audit log with auto-maintained views (recommended)";
    private const string UpsertChoice =
        "upsert     — mirror source tables, keeping only the latest state of each row";

    public static async Task<Result> RunAsync(string configPath, CancellationToken ct)
    {
        AnsiConsole.MarkupLine("[bold yellow]Server Configuration[/]");
        AnsiConsole.MarkupLine("[grey]Configure how EDS streams events. Press Enter to accept the highlighted default.[/]");
        AnsiConsole.WriteLine();

        // ── 1. Driver mode ────────────────────────────────────────────────────
        var modeChoice = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("[bold]Driver mode[/] — how should events be written to your database?")
                .HighlightStyle(new Style(Color.Green))
                .AddChoices(TimeSeriesChoice, UpsertChoice));

        var mode    = modeChoice == TimeSeriesChoice ? DriverMode.TimeSeries : DriverMode.Upsert;
        var modeStr = mode == DriverMode.TimeSeries ? "timeseries" : "upsert";
        await ConfigFileHelper.SetValueAsync(configPath, "driver_mode", modeStr);

        // ── 2. Events schema (timeseries only) ────────────────────────────────
        var eventsSchema = "eds_events";
        if (mode == DriverMode.TimeSeries)
        {
            eventsSchema = AnsiConsole.Prompt(
                new TextPrompt<string>("[bold]Events schema[/] — database schema to hold events tables:")
                    .DefaultValue("eds_events"));
            await ConfigFileHelper.SetValueAsync(configPath, "events_schema", eventsSchema);
        }

        // ── 3–5. Flush tuning ─────────────────────────────────────────────────
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[grey]Advanced flush tuning — controls how events are batched before writing.[/]");

        var minSecs = AnsiConsole.Prompt(
            new TextPrompt<int>("[bold]Min flush interval[/] (seconds) — minimum time to accumulate events before writing:")
                .DefaultValue(2)
                .ValidationErrorMessage("[red]Please enter a positive integer.[/]")
                .Validate(v => v > 0
                    ? ValidationResult.Success()
                    : ValidationResult.Error("Must be greater than zero.")));
        await ConfigFileHelper.SetValueAsync(configPath, "min_pending_latency", minSecs.ToString());

        var maxSecs = AnsiConsole.Prompt(
            new TextPrompt<int>("[bold]Max flush interval[/] (seconds) — flush is forced after this interval regardless of batch size:")
                .DefaultValue(30)
                .ValidationErrorMessage("[red]Please enter a positive integer greater than the min interval.[/]")
                .Validate(v => v > minSecs
                    ? ValidationResult.Success()
                    : ValidationResult.Error($"Must be greater than min interval ({minSecs}s).")));
        await ConfigFileHelper.SetValueAsync(configPath, "max_pending_latency", maxSecs.ToString());

        var maxAck = AnsiConsole.Prompt(
            new TextPrompt<int>("[bold]Max batch size[/] — maximum number of events to accumulate before an immediate flush:")
                .DefaultValue(16384)
                .ValidationErrorMessage("[red]Please enter a positive integer.[/]")
                .Validate(v => v > 0
                    ? ValidationResult.Success()
                    : ValidationResult.Error("Must be greater than zero.")));
        await ConfigFileHelper.SetValueAsync(configPath, "max_ack_pending", maxAck.ToString());

        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold green]Configuration saved.[/]");
        AnsiConsole.WriteLine();

        return new Result(mode, eventsSchema, minSecs, maxSecs, maxAck);
    }
}
