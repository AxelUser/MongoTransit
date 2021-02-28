using System;

namespace MongoTransit.Transit
{
    public record IterativeTransitOptions(string Field, bool StopOnError, DateTime ForcedCheckpoint);
}