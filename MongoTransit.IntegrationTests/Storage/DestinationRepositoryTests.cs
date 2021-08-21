using System;
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
using Moq;
using Serilog;
using Xunit;

namespace MongoTransit.IntegrationTests.Storage
{
    public class DestinationRepositoryTests: RepositoriesTestBase
    {
        
        private readonly IMongoCollection<BsonDocument> _destCollection;
        private readonly DestinationRepository _sut;
        
        public DestinationRepositoryTests()
        {
            var (_, collection) = CreateConnection(nameof(DestinationRepositoryTests));
            _destCollection = collection;

            _sut = new DestinationRepository(_destCollection, new Mock<ILogger>().Object);
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
            afterReplace.Should().BeEquivalentTo(new[] { replacement });
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
            afterReplace.Should().BeEquivalentTo(new[] { inserted });
        }

        #endregion

        #region FindDocumentAsync

        [Fact]
        public async Task FindDocumentAsync_ShouldReturnNull_EmptyCollection()
        {
            // Act
            var actual = await _sut.FindDocumentAsync(new BsonDocument
            {
                ["_id"] = Fixture.Create<string>(),
                ["Value"] = Fixture.Create<string>(),
            }, CancellationToken.None);

            // Assert
            actual.Should().BeNull();
        }
        
        [Fact]
        public async Task FindDocumentAsync_ShouldReturnNull_MissingDocument()
        {
            // Arrange
            await _destCollection.InsertOneAsync(new BsonDocument
            {
                ["_id"] = Fixture.Create<string>(),
                ["Value"] = Fixture.Create<string>(),
            });
            
            // Act
            var actual = await _sut.FindDocumentAsync(new BsonDocument
            {
                ["_id"] = Fixture.Create<string>(),
                ["Value"] = Fixture.Create<string>(),
            }, CancellationToken.None);

            // Assert
            actual.Should().BeNull();
        }
        
        [Fact]
        public async Task FindDocumentAsync_ShouldReturnDocument_CollectionHasSameDocument()
        {
            // Arrange
            var target = new BsonDocument
            {
                ["Value"] = Fixture.Create<string>(),
            };
            await _destCollection.InsertManyAsync(Enumerable.Range(0, 10).Select(_ => new BsonDocument
            {
                ["Value"] = Fixture.Create<string>()
            }).Append(target));
            
            // Act
            var actual = await _sut.FindDocumentAsync(target, CancellationToken.None);

            // Assert
            actual.Should().BeEquivalentTo(target);
        }
        
        [Fact]
        public async Task FindDocumentAsync_ShouldReturnDocumentWithSameId_CollectionHasDocumentWithId()
        {
            // Arrange
            var target = new BsonDocument
            {
                ["Value"] = Fixture.Create<string>(),
            };
            await _destCollection.InsertManyAsync(Enumerable.Range(0, 10).Select(_ => new BsonDocument
            {
                ["Value"] = Fixture.Create<string>()
            }).Append(target));
            
            // Act
            var actual = await _sut.FindDocumentAsync(new BsonDocument
            {
                ["_id"] = target["_id"],
                ["Value"] = Fixture.Create<string>(),
            }, CancellationToken.None);

            // Assert
            actual.Should().BeEquivalentTo(target);
        }

        #endregion
    }
}