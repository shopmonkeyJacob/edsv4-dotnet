using EDS.Core.Utilities;
using EDS.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Spectre.Console;
using System.Text.Json;

namespace EDS.Cli;

/// <summary>
/// Handles first-time enrollment with Shopmonkey HQ.
/// Mirrors the Go serverCmd enrollment loop and enroll subcommand logic.
/// </summary>
public static class EnrollmentFlow
{
    /// <summary>
    /// Checks if the server is enrolled. If not, prompts the user for a
    /// one-time enrollment code, calls the Shopmonkey API, and writes the
    /// resulting token + server_id to config.toml.
    /// Returns <c>(enrolled: false, justEnrolled: false)</c> if the user aborted.
    /// Returns <c>(enrolled: true, justEnrolled: false)</c> if already enrolled.
    /// Returns <c>(enrolled: true, justEnrolled: true)</c> after fresh enrollment.
    /// </summary>
    public static async Task<(bool enrolled, bool justEnrolled)> EnrollIfNeededAsync(
        string dataDir,
        string configPath,
        CancellationToken ct)
    {
        // Load existing config to check for server_id
        var config = new ConfigurationBuilder()
            .AddTomlFile(configPath, optional: true)
            .AddEnvironmentVariables("EDS_")
            .Build();

        if (!string.IsNullOrWhiteSpace(config["server_id"]))
            return (enrolled: true, justEnrolled: false); // Already enrolled

        AnsiConsole.MarkupLine("[bold yellow]Welcome to Shopmonkey EDS![/]");
        AnsiConsole.WriteLine();

        while (true)
        {
            var code = AnsiConsole.Ask<string>("[bold white]Enter one-time enrollment code:[/]").Trim();

            if (string.IsNullOrEmpty(code))
            {
                AnsiConsole.MarkupLine("[red]No enrollment code provided. Exiting.[/]");
                return (enrolled: false, justEnrolled: false);
            }

            var apiUrl = GetApiUrlFromCode(code);

            try
            {
                AnsiConsole.MarkupLine($"[grey]Enrolling with {apiUrl}...[/]");
                var (token, serverId) = await CallEnrollApiAsync(apiUrl, code, ct);
                await WriteConfigAsync(configPath, token, serverId);
                AnsiConsole.MarkupLine("[bold green]Enrollment successful![/]");
                AnsiConsole.MarkupLine($"[grey]Server ID: {serverId}[/]");
                AnsiConsole.WriteLine();
                return (enrolled: true, justEnrolled: true);
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Enrollment failed:[/] {ex.Message.EscapeMarkup()}");
                AnsiConsole.WriteLine();
                // Loop back to prompt
            }
        }
    }

    private static string GetApiUrlFromCode(string code)
    {
        return char.ToUpperInvariant(code[0]) switch
        {
            'S' => "https://sandbox-api.shopmonkey.cloud",
            'E' => "https://edge-api.shopmonkey.cloud",
            'L' => "http://localhost:3101",
            _   => "https://api.shopmonkey.cloud"  // 'P' or anything else → production
        };
    }

    private static async Task<(string token, string serverId)> CallEnrollApiAsync(
        string apiUrl, string code, CancellationToken ct)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        var response = await RetryHelper.ExecuteAsync(
            innerCt => http.GetAsync($"{apiUrl}/v3/eds/internal/enroll/{Uri.EscapeDataString(code)}", innerCt),
            operationName: "enroll", ct: ct);

        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
            throw new InvalidOperationException("Invalid enrollment code, or it has already been used.");

        response.EnsureSuccessStatusCode();

        var json = await response.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        if (!root.GetProperty("success").GetBoolean())
        {
            var message = root.TryGetProperty("message", out var msg)
                ? msg.GetString() ?? "Enrollment failed."
                : "Enrollment failed.";
            throw new InvalidOperationException(message);
        }

        var data = root.GetProperty("data");
        var token    = data.GetProperty("token").GetString()
            ?? throw new InvalidOperationException("No token in enrollment response.");
        var serverId = data.GetProperty("serverId").GetString()
            ?? throw new InvalidOperationException("No serverId in enrollment response.");

        return (token, serverId);
    }

    private static async Task WriteConfigAsync(string configPath, string token, string serverId)
    {
        await ConfigFileHelper.SetValueAsync(configPath, "token", token);
        await ConfigFileHelper.SetValueAsync(configPath, "server_id", serverId);
    }
}
