using System;

namespace MongoTransit
{
    public record IterativeTransitOptions(string Field, bool StopOnError, DateTime ForcedCheckpoint);
}