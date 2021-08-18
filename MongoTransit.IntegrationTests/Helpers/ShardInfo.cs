namespace MongoTransit.IntegrationTests.Helpers
{
    public class ShardInfo
    {
        public string Id { get; set; }
        
        public string[] Zones { get; set; }
        
        public string ReplicaSet { get; set; }
        
        public string[] Hosts { get; set; }
    }
}