namespace MongoTransit.Transit
{
    public record CollectionTransitOptions(string SourceConnectionString, string DestinationConnectionString,
        string Database, string Collection, string[]? UpsertFields,
        int Workers, int BatchSize,
        IterativeTransitOptions? IterativeTransferOptions);
}