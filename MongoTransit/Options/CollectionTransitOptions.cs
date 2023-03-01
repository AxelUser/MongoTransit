namespace MongoTransit.Options
{
    public record CollectionTransitOptions(string SourceConnectionString, string DestinationConnectionString,
        string Database, string Collection,
        ShardKeyOptions? ShardKeyOptions,
        int Workers, int BatchSize, bool Upsert,
        IterativeTransitOptions? IterativeTransferOptions);
}