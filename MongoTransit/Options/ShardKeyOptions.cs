namespace MongoTransit.Options;

public record ShardKeyOptions(string[]? Fields, bool FromDestination);