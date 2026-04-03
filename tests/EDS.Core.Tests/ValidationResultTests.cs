using EDS.Core.Abstractions;

namespace EDS.Core.Tests;

public class ValidationResultTests
{
    // ── ValidationResult ──────────────────────────────────────────────────────

    [Fact]
    public void Success_IsValid_True()
    {
        Assert.True(ValidationResult.Success.IsValid);
        Assert.Empty(ValidationResult.Success.Errors);
    }

    [Fact]
    public void Success_IsSingletonInstance()
    {
        Assert.Same(ValidationResult.Success, ValidationResult.Success);
    }

    [Fact]
    public void Failure_WithErrors_IsNotValid()
    {
        var errors = new[] { new FieldError { Field = "url", Message = "required" } };
        var result = ValidationResult.Failure(errors);

        Assert.False(result.IsValid);
        Assert.Single(result.Errors);
    }

    [Fact]
    public void Failure_ExposesAllErrors()
    {
        var errors = new[]
        {
            new FieldError { Field = "host",     Message = "required" },
            new FieldError { Field = "password", Message = "too short" },
        };

        var result = ValidationResult.Failure(errors);

        Assert.Equal(2, result.Errors.Count);
    }

    [Fact]
    public void FailureForField_CreatesExpectedError()
    {
        var result = ValidationResult.FailureForField("username", "must not be empty");

        Assert.False(result.IsValid);
        Assert.Single(result.Errors);
        Assert.Equal("username", result.Errors[0].Field);
        Assert.Equal("must not be empty", result.Errors[0].Message);
    }

    // ── FieldError ────────────────────────────────────────────────────────────

    [Fact]
    public void FieldError_ToString_ContainsFieldAndMessage()
    {
        var err = new FieldError { Field = "url", Message = "invalid format" };
        Assert.Equal("url: invalid format", err.ToString());
    }
}
