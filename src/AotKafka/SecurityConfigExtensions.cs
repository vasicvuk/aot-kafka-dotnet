using System;
using System.Collections.Generic;

namespace AotKafka;

internal static class SecurityConfigExtensions
{
    public static void ApplySecurity(
        this Dictionary<string, string> config,
        SecurityProtocol securityProtocol,
        SaslMechanism saslMechanism,
        string? saslUsername,
        string? saslPassword,
        string ownerName)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentException.ThrowIfNullOrEmpty(ownerName);

        var protocolValue = securityProtocol switch
        {
            SecurityProtocol.Plaintext => "plaintext",
            SecurityProtocol.Ssl => "ssl",
            SecurityProtocol.SaslPlaintext => "sasl_plaintext",
            SecurityProtocol.SaslSsl => "sasl_ssl",
            _ => throw new ArgumentOutOfRangeException(nameof(securityProtocol), securityProtocol, "Unsupported security protocol")
        };

        config["security.protocol"] = protocolValue;

        var usesSasl = securityProtocol is SecurityProtocol.SaslPlaintext or SecurityProtocol.SaslSsl;
        if (!usesSasl)
        {
            if (!string.IsNullOrWhiteSpace(saslUsername) || !string.IsNullOrWhiteSpace(saslPassword))
            {
                throw new ArgumentException(
                    $"{ownerName} provided SASL credentials but security protocol is {securityProtocol}. Set SecurityProtocol to SaslPlaintext or SaslSsl to enable SASL.",
                    nameof(securityProtocol));
            }

            return;
        }

        if (string.IsNullOrWhiteSpace(saslUsername))
        {
            throw new ArgumentException($"{ownerName} requires a non-empty {nameof(saslUsername)} when using SASL.", nameof(saslUsername));
        }

        if (string.IsNullOrWhiteSpace(saslPassword))
        {
            throw new ArgumentException($"{ownerName} requires a non-empty {nameof(saslPassword)} when using SASL.", nameof(saslPassword));
        }

        config["sasl.mechanisms"] = saslMechanism switch
        {
            SaslMechanism.Plain => "PLAIN",
            SaslMechanism.ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism.ScramSha512 => "SCRAM-SHA-512",
            _ => throw new ArgumentOutOfRangeException(nameof(saslMechanism), saslMechanism, "Unsupported SASL mechanism")
        };

        config["sasl.username"] = saslUsername!;
        config["sasl.password"] = saslPassword!;
    }
}

