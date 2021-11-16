using System;

namespace MongoTransit.Configuration.Options
{
    public record IterativeTransitOptions(string Field, TimeSpan Offset, DateTime? ForcedCheckpoint);
}