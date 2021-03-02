using System;

namespace MongoTransit.Transit
{
    public record IterativeTransitOptions(string Field, DateTime? ForcedCheckpoint);
}