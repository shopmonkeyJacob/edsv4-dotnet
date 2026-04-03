using System.Reflection;

namespace EDS.Cli;

internal static class EdsVersion
{
    // Set at build time via: dotnet publish -p:Version=v1.2.3-alpha
    // Uses AssemblyInformationalVersion so pre-release suffixes (e.g. -alpha) are preserved.
    // The SDK appends +{gitCommitHash} as SemVer build metadata — strip it so HQ receives
    // a clean version string (e.g. "0.8.3-beta" not "0.8.3-beta+decc83c4aceb...").
    // Falls back to "dev" when running from source without an explicit version.
    public static string Current { get; } =
        (typeof(EdsVersion).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()
            ?.InformationalVersion
            ?? "dev")
        .Split('+')[0];

    // The Shopmonkey PGP public key — embedded here to match Go's behavior
    public const string ShopmonkeyPublicPgpKey = """
        -----BEGIN PGP PUBLIC KEY BLOCK-----

        mDMEZqbX1BYJKwYBBAHaRw8BAQdArMIL32vatKdxrJK2F/aKN+q3hS73CPnUdgpJ
        KrxyOYu0LFNob3Btb25rZXksIEluYy4gPGVuZ2luZWVyaW5nQHNob3Btb25rZXku
        aW8+iJMEExYKADsWIQTJlG6Z3WVVBa1f6dL+z/SRXnv0kgUCZqbX1AIbAwULCQgH
        AgIiAgYVCgkICwIEFgIDAQIeBwIXgAAKCRD+z/SRXnv0kj2FAP9AfaBMaXBGr9OP
        vQXHD/dC9DVqu5AWJns98A6OAMxYDAD+IDfjZGf9SsBal9/HE5j6FbuRCcl52Jwx
        97f7OrIAhQa4OARmptfUEgorBgEEAZdVAQUBAQdAI7jC9e+tOyLA+k8JWvZu666l
        LjXvPznbu9I2dkaLMzcDAQgHiHgEGBYKACAWIQTJlG6Z3WVVBa1f6dL+z/SRXnv0
        kgUCZqbX1AIbDAAKCRD+z/SRXnv0kuoWAP91V3SLcNLaXndipxJJ/Z5oQjsyuTDy
        3rhqtxmg+EsXVgD/SFc612ihYO2/DFooZ04EU4wwFjj/0u4rxcUdj04u+AI=
        =r0eF
        -----END PGP PUBLIC KEY BLOCK-----
        """;
}
