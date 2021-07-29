using System.Threading.Tasks;
using MongoDB.Driver;
using MongoTransit.IntegrationTests.Helpers;
using Xunit;

namespace MongoTransit.IntegrationTests
{
    public class MongoHelperTest
    {
        private readonly MongoHelper _helper;

        public MongoHelperTest()
        {
            _helper = new MongoHelper(IntegrationHelper.CreateClient());
        }
        
        [Fact]
        public async Task CreateShardedCollection_ShouldCompleteWithoutErrors_PassedValidParameters()
        {
            await _helper.CreateShardedCollectionAsync("TestDb", "TestCollection", nameof(Entity.ShardedKey));
        }
    }
}