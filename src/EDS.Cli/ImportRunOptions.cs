using EDS.Core.Abstractions;

namespace EDS.Cli;

/// <summary>
/// All parameters needed to run the import pipeline.
/// Shared between the <c>import</c> command and the post-enrollment flow inside
/// the <c>server</c> command.
/// </summary>
internal sealed record ImportRunOptions
{
    public required string DataDir    { get; init; }
    public required string ConfigPath { get; init; }
    public required bool   Verbose    { get; init; }
    public required string DriverUrl  { get; init; }
    public required string ApiKey     { get; init; }

    /// <summary>How the SQL driver writes events. Default: Upsert.</summary>
    public DriverMode Mode         { get; init; } = DriverMode.Upsert;

    /// <summary>Events schema name used in TimeSeries mode. Default: eds_events.</summary>
    public string EventsSchema     { get; init; } = "eds_events";

    public string?   JobId       { get; init; }
    public string?   Dir         { get; init; }
    public string[]  Only        { get; init; } = [];
    public string[]  CompanyIds  { get; init; } = [];
    public string[]  LocationIds { get; init; } = [];
    public long?     TimeOffset  { get; init; }
    public int       Parallel    { get; init; } = 4;
    public bool      DryRun      { get; init; }
    public bool      NoConfirm   { get; init; }
    public bool      NoCleanup   { get; init; }
    public bool      NoDelete    { get; init; }
    public bool      SchemaOnly  { get; init; }
    public bool      Single      { get; init; }

    /// <summary>
    /// When true, load the last saved checkpoint from the tracker and resume
    /// the import from the first unfinished file. Implies <see cref="NoDelete"/>
    /// and <see cref="NoCleanup"/>.
    /// </summary>
    public bool      Resume      { get; init; }

    /// <summary>
    /// When true, a note is written to the import log indicating this import
    /// was triggered by the post-enrollment prompt rather than the import command.
    /// </summary>
    public bool IsPostEnrollment { get; init; }

    /// <summary>
    /// Override the import log file path. When null, a timestamped filename is generated
    /// automatically. Set this when the caller needs to include the log path in a response
    /// before the import starts (e.g. the configure+backfill notification response).
    /// </summary>
    public string? LogFile { get; init; }
}
