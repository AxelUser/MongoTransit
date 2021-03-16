using System;

namespace MongoTransit.Transit
{
    public record IterativeTransitOptions(string Field, TimeSpan Offset, DateTime? ForcedCheckpoint);
}