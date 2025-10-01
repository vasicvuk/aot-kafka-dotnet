using Xunit;

namespace AotKafka.Tests;

public class AdminConfigTests
{
    [Fact]
    public void AdminConfig_ToNativeConfig_WithSasl_ShouldPopulateValues()
    {
        var config = new AdminConfig
        {
            BootstrapServers = "example:9093",
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = "admin",
            SaslPassword = "secret"
        };

        var nativeConfig = config.ToNativeConfig();

        Assert.Equal("sasl_ssl", nativeConfig["security.protocol"]);
        Assert.Equal("PLAIN", nativeConfig["sasl.mechanisms"]);
        Assert.Equal("admin", nativeConfig["sasl.username"]);
        Assert.Equal("secret", nativeConfig["sasl.password"]);
    }
}
