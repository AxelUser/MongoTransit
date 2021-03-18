using System;

namespace MongoTransit.Options
{
    public record IterativeTransitOptions(string Field, TimeSpan Offset, DateTime? ForcedCheckpoint);
}