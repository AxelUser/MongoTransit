namespace MongoTransit.Options
{
    public record CollectionTransitOptions(string SourceConnectionString, string DestinationConnectionString,
        string Database, string Collection,
        string[]? KeyFields, bool FetchKeyFromDestination,
        int Workers, int BatchSize, bool Upsert,
        IterativeTransitOptions? IterativeTransferOptions);
}