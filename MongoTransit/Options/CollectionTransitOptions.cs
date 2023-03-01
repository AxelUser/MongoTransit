namespace MongoTransit.Options
{
    public record CollectionTransitOptions(string SourceConnectionString, string DestinationConnectionString,
        string Database, string Collection,
        ShardedKeyOptions? ShardedKeyOptions,
        int Workers, int BatchSize, bool Upsert,
        IterativeTransitOptions? IterativeTransferOptions);
}