using Serilog;
using Serilog.Events;

namespace MongoTransit.IntegrationTests;

public static class TestLoggerFactory
{
    public static ILogger Create(string testName)
    {
        return new LoggerConfiguration()
            .MinimumLevel.Debug()
            .Enrich.WithProperty("Scope", testName)
            .WriteTo.Console(LogEventLevel.Debug,
                "[{Timestamp:HH:mm:ss} {Level:u3}][{Scope}] {Message:lj}{NewLine}{Exception}")
            .CreateLogger(); 
    }
}