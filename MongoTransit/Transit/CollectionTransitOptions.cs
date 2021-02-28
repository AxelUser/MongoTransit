namespace MongoTransit.Transit
{
    public record CollectionTransitOptions(string FromConnectionString, string ToConnectionString,
        string Database, string Collection, string[] UpsertFields,
        int Workers, int BatchSize,
        IterativeTransitOptions? IterativeTransferOptions);
}