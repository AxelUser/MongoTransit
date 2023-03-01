namespace MongoTransit.Options;

public record ShardedKeyOptions(string[]? Fields, bool FromDestination);