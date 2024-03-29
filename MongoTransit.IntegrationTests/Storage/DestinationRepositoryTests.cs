using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Extensions;
using MongoTransit.IntegrationTests.Extensions;
using MongoTransit.Storage.Destination;
using Xunit;

namespace MongoTransit.IntegrationTests.Storage
{
    public class DestinationRepositoryTests: RepositoriesTestBase
    {
        
        private readonly IMongoCollection<BsonDocument> _destCollection;
        private readonly DestinationRepository _sut;
        
        public DestinationRepositoryTests()
        {
            _destCollection = CreateConnection(nameof(DestinationRepositoryTests));;

            _sut = new DestinationRepository(_destCollection, TestLoggerFactory.Create(nameof(DestinationRepositoryTests)));
        }

        #region FindLastCheckpointAsync

        [Fact]
        public async Task FindLastCheckpointAsync_ShouldReturnNull_EmptyCollection()
        {
            // Act
            var actual = await _sut.FindLastCheckpointAsync("Modified", CancellationToken.None);
            
            // Assert
            actual.Should().BeNull();
        }
        
        [Fact]
        public async Task FindLastCheckpointAsync_ShouldReturnNull_NoCheckpointValueInCollection()
        {
            // Arrange
            await _destCollection.InsertManyAsync(Enumerable.Range(0, 10)
                .Select(_ => new BsonDocument
                {
                    ["Value"] = Fixture.Create<string>()
                }));
            
            // Act
            var actual = await _sut.FindLastCheckpointAsync("Modified", CancellationToken.None);
            
            // Assert
            actual.Should().BeNull();
        }
        
        [Fact]
        public async Task FindLastCheckpointAsync_ShouldReturnMaxCheckpointValue_HasCheckpointValuesInCollection()
        {
            // Arrange
            var lastCheckpoint = Fixture.CreateMongoDbDate().ToUniversalTime();
            await _destCollection.InsertManyAsync(Enumerable.Range(0, 10)
                .Select(offset => new BsonDocument
                {
                    ["Modified"] = new BsonDateTime(lastCheckpoint.AddSeconds(-offset))
                }));

            // Act
            var actual = await _sut.FindLastCheckpointAsync("Modified", CancellationToken.None);

            // Assert
            actual.Should().Be(lastCheckpoint);
        }

        #endregion

        #region ReplaceManyAsync

        [Fact]
        public async Task ReplaceManyAsync_ShouldNotDoAnyInsertions_EmptyBulk()
        {
            
            // Act
            await _sut.ReplaceManyAsync( new List<ReplaceOneModel<BsonDocument>>(), CancellationToken.None);
            
            // Assert
            var actual = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            actual.Should().BeEmpty();
        }
        
        [Fact]
        public async Task ReplaceManyAsync_ShouldNotDoAnyInsertions_NullValueForBulk()
        {
            
            // Act
            await _sut.ReplaceManyAsync( null, CancellationToken.None);
            
            // Assert
            var actual = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            actual.Should().BeEmpty();
        }
        
        [Fact]
        public async Task ReplaceManyAsync_ShouldPerformReplacements_ReplaceById()
        {
            await _destCollection.InsertManyAsync(Enumerable.Range(0, 10)
                .Select(_ => new BsonDocument
                {
                    ["Value"] = Fixture.Create<string>()
                }));
            var replacements = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList()
                .Select(document =>
                {
                    document["Value"] = Fixture.Create<string>();
                    return document;
                }).ToList();
            var replaceModels = replacements
                .Select(document =>
                    new ReplaceOneModel<BsonDocument>(document.GetFilterBy("_id"), document))
                .ToList();
            
            // Act
            await _sut.ReplaceManyAsync( replaceModels, CancellationToken.None);
            
            // Assert
            var actual = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            actual.Should().BeEquivalentTo(replacements);
        }
        
        [Fact]
        public async Task ReplaceManyAsync_ShouldPerformReplacements_ReplaceByField()
        {
            await _destCollection.InsertManyAsync(Enumerable.Range(0, 10)
                .Select(_ => new BsonDocument
                {
                    ["Key"] = Fixture.Create<string>(),
                    ["Value"] = Fixture.Create<string>(),
                }));
            var replacements = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList()
                .Select(document =>
                {
                    document["Value"] = Fixture.Create<string>();
                    return document;
                }).ToList();
            var replaceModels = replacements
                .Select(document =>
                    new ReplaceOneModel<BsonDocument>(document.GetFilterBy("Key"), document))
                .ToList();
            
            // Act
            await _sut.ReplaceManyAsync( replaceModels, CancellationToken.None);
            
            // Assert
            var actual = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            actual.Should().BeEquivalentTo(replacements);
        }
        
        [Fact]
        public async Task ReplaceManyAsync_ShouldPerformInsertions_ModelHasUpsertOption()
        {
            var newDocuments = Enumerable.Range(0, 10)
                .Select(_ => new BsonDocument
                {
                    ["_id"] = Fixture.Create<string>(),
                    ["Value"] = Fixture.Create<string>(),
                }).ToList();
            var replaceModels = newDocuments
                .Select(document =>
                    new ReplaceOneModel<BsonDocument>(document.GetFilterBy("_id"), document)
                    {
                        IsUpsert = true
                    })
                .ToList();
            
            // Act
            await _sut.ReplaceManyAsync( replaceModels, CancellationToken.None);
            
            // Assert
            var actual = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            actual.Should().BeEquivalentTo(newDocuments);
        }

        #endregion

        #region DeleteAllDocumentsAsync

        [Fact]
        public async Task DeleteAllDocumentsAsync_ShouldNotDoAnyDeletions_CollectionIsEmpty()
        {
            // Arrange
            var existing = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            
            // Act
            await _sut.DeleteAllDocumentsAsync(CancellationToken.None);
            
            // Assert
            var afterDeletion = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            afterDeletion.Should().BeEmpty().And.BeEquivalentTo(existing);
        }
        
        [Fact]
        public async Task DeleteAllDocumentsAsync_ShouldDeleteAllDocuments_CollectionHasDocuments()
        {
            // Arrange
            await _destCollection.InsertManyAsync(Enumerable.Range(0, 10)
                .Select(_ => new BsonDocument
                {
                    ["Value"] = Fixture.Create<string>()
                }));
            var beforeDeletion = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            
            // Act
            await _sut.DeleteAllDocumentsAsync(CancellationToken.None);
            
            // Assert
            var afterDeletion = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            beforeDeletion.Should().NotBeEmpty();
            afterDeletion.Should().BeEmpty();
        }

        #endregion

        #region ReplaceDocumentAsync

        [Fact]
        public async Task ReplaceDocumentAsync_ShouldReplaceDocument_CollectionHasOriginalDocument()
        {
            // Arrange
            var inserted = new BsonDocument
            {
                ["Value"] = Fixture.Create<string>()
            };
            
            await _destCollection.InsertOneAsync(inserted);

            // Act
            var replacement = new BsonDocument
            {
                ["_id"] = inserted["_id"],
                ["Value"] = "replaced"
            };
            await _sut.ReplaceDocumentAsync(new ReplaceOneModel<BsonDocument>(replacement.GetFilterBy("_id"), replacement),
                CancellationToken.None);
            
            // Assert
            var afterReplace = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            afterReplace.Should().HaveCount(1).And.AllBeEquivalentTo(replacement);
        }
        
        [Fact]
        public async Task ReplaceDocumentAsync_ShouldNotReplaceOrInsertDocument_CollectionMissingOriginalDocument()
        {
            // Arrange
            var inserted = new BsonDocument
            {
                ["_id"] = "exist",
                ["Value"] = Fixture.Create<string>()
            };
            
            await _destCollection.InsertOneAsync(inserted);

            // Act
            var replacement = new BsonDocument
            {
                ["_id"] = "not_exist",
                ["Value"] = Fixture.Create<string>()
            };
            await _sut.ReplaceDocumentAsync(new ReplaceOneModel<BsonDocument>(replacement.GetFilterBy("_id"), replacement),
                CancellationToken.None);
            
            // Assert
            var afterReplace = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            afterReplace.Should().HaveCount(1).And.AllBeEquivalentTo(inserted);
        }

        #endregion

        #region GetFieldsAsync

        [Fact]
        public async Task GetFieldsAsync_ShouldReturnEmptyList_CollectionIsEmpty()
        {
            // Act
            var actual = await _sut.GetFieldsAsync(new []
            {
                new BsonDocument
                {
                    ["_id"] = Fixture.Create<string>(),
                    ["Value"] = Fixture.Create<string>(),
                }
            }, null, CancellationToken.None);

            // Assert
            actual.Should().BeEmpty();
        }
        
        [Fact]
        public async Task GetFieldsAsync_ShouldReturnEmptyList_MissingDocument()
        {
            // Arrange
            await _destCollection.InsertOneAsync(new BsonDocument
            {
                ["_id"] = Fixture.Create<string>(),
                ["Value"] = Fixture.Create<string>(),
            });
            
            // Act
            var actual = await _sut.GetFieldsAsync(new []
            {
                new BsonDocument
                {
                    ["_id"] = Fixture.Create<string>(),
                    ["Value"] = Fixture.Create<string>(),
                }
            }, null, CancellationToken.None);

            // Assert
            actual.Should().BeEmpty();
        }
        
        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        public async Task GetFieldsAsync_ShouldReturnOnlyDocumentsWithSameId_CollectionHasDocumentsWithTargetedIds_NoFieldsProvided(int documentsCount)
        {
            // Arrange
            var targets = Enumerable.Range(0, documentsCount).Select(_ => new BsonDocument
            {
                ["Value"] = Fixture.Create<string>(),
            }).ToList();
            await _destCollection.InsertManyAsync(Enumerable.Range(0, 10).Select(_ => new BsonDocument
            {
                ["Value"] = Fixture.Create<string>()
            }).Concat(targets));
            
            // Act
            var actual = await _sut.GetFieldsAsync(targets, null, CancellationToken.None);

            // Assert
            actual.Should().BeEquivalentTo(targets.Select(d => new BsonDocument("_id", d["_id"])));
        }
        
        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        public async Task GetFieldsAsync_ShouldReturnOnlyIdAndSpecifiedFields_CollectionHasDocumentsWithTargetedIds_FieldsProvided(int documentsCount)
        {
            // Arrange
            var fields = new[] { "Key1", "Key2" };
            var targets = Enumerable.Range(0, documentsCount).Select(_ => new BsonDocument
            {
                ["Key1"] = Fixture.Create<string>(),
                ["Key2"] = Fixture.Create<string>(),
                ["Value"] = Fixture.Create<string>(),
            }).ToList();
            await _destCollection.InsertManyAsync(Enumerable.Range(0, 10).Select(_ => new BsonDocument
            {
                ["Value"] = Fixture.Create<string>()
            }).Concat(targets));
            
            // Act
            var actual = await _sut.GetFieldsAsync(targets, fields, CancellationToken.None);

            // Assert
            actual.Select(d => new BsonDocument("_id", d["_id"])).Should()
                .BeEquivalentTo(targets.Select(d => new BsonDocument("_id", d["_id"])));

            foreach (var actualDoc in actual)
            {
                actualDoc.Names.Should().BeEquivalentTo(fields.Append("_id"));
            }
        }

        #endregion
    }
}