using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using Rook.Framework.Core.Common;
using Rook.Framework.Core.StructureMap;
using Rook.Framework.Core.StructureMap.Registries;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Moq;
using Rook.Framework.Core.AmazonKinesisFirehose;
using Rook.Framework.MongoDb.Data;
using Rook.Framework.MongoDb.TestUtils;
using StructureMap;

namespace Rook.Framework.MongoDb.Tests.Unit.Data
{
    [TestClass]
    public class MongoStoreTests
    {
        private Mock<ILogger> _logger;
        private Mock<IConfigurationManager> _configurationManager;
        private Mock<Framework.MongoDb.Data.IMongoClient> _mongoClient;
        private Mock<IContainerFacade> _containerFacade;
        private Mock<IAmazonFirehoseProducer> _amazonFirehoseProducer;

        [TestInitialize]
        public void BeforeEachTest()
        {
            _logger = new Mock<ILogger>();
            _configurationManager = new Mock<IConfigurationManager>();
            _configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> { { "MongoDatabaseUri", "" }, { "MongoDatabaseName", "" } });
            _mongoClient = new Mock<Framework.MongoDb.Data.IMongoClient>();
            _containerFacade = new Mock<IContainerFacade>();
            _amazonFirehoseProducer = new Mock<IAmazonFirehoseProducer>();
        }

        private MongoStore Sut => new MongoStore(_logger.Object, _configurationManager.Object, _mongoClient.Object, _containerFacade.Object, _amazonFirehoseProducer.Object);

        [TestMethod]
        public void MongoStore_WhenInstantiated_CallsMongoClientCreate()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});
            
            new MongoStore(_logger.Object, _configurationManager.Object, _mongoClient.Object, _containerFacade.Object, _amazonFirehoseProducer.Object);

            _mongoClient.Verify(x => x.Create(It.IsAny<string>()), Times.Once);
        }

        [TestMethod]
        public void GetCollection_WhenCalledWithExpiryIndexSetup_DoesNotCreateExpiryIndex()
        {
            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var indexCalled = false;
            asyncCursor.Setup(x => x.MoveNext(It.IsAny<CancellationToken>())).Returns(() =>
            {
                var localIndexCalled = !indexCalled;
                indexCalled = true;
                return localIndexCalled;
            });
            asyncCursor.SetupGet(x => x.Current).Returns(() => new List<BsonDocument>
            {
                new BsonDocument(new Dictionary<string, BsonDocument>
                {
                    {"key", new BsonDocument(new BsonElement("ExpiresAt", new BsonDocument()))},
                    {"expireAfterSeconds", new BsonDocument()}
                })
            });
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);
            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            mongoStore.GetCollection<DataEntity>();

            mongoIndexManager.Verify(x => x.CreateOne(It.IsAny<IndexKeysDefinition<DataEntity>>(), It.IsAny<CreateIndexOptions>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [TestMethod]
        public void GetCollection_WhenCalledWithNoExpiryIndexSetup_CreatesExpiryIndex()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});
            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);
            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            mongoStore.GetCollection<DataEntity>();

            mongoIndexManager.Verify(x => x.CreateOne(It.IsAny<IndexKeysDefinition<DataEntity>>(), It.IsAny<CreateIndexOptions>(), It.IsAny<CancellationToken>()), Times.Once);
        }


        [TestMethod]
        public void GetCollection_WhenCalled_CallsGetCollectionAndMongoClientGetDatabase()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});
            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);
            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            var result = mongoStore.GetCollection<DataEntity>();

            mongoDatabase.Verify(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()), Times.Once);
            _mongoClient.Verify(x => x.Create(It.IsAny<string>()), Times.Once);
            Assert.IsTrue(result is IMongoCollection<DataEntity>);
        }

        [TestMethod]
        public void QueryableCollection_WhenCalled_ReturnsQueryableCollection()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});
            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);
            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            var result = mongoStore.QueryableCollection<DataEntity>();

            mongoDatabase.Verify(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()), Times.Once);
            _mongoClient.Verify(x => x.Create(It.IsAny<string>()), Times.Once);
            Assert.IsTrue(result is IQueryable<DataEntity>);
        }

        [TestMethod]
        public void Count_WhenCalledWithNoCriteria_ReturnsCollectionCount()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});

            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);
            mongoCollection.Setup(x => x.Count(It.IsAny<FilterDefinition<DataEntity>>(), It.IsAny<CountOptions>(), It.IsAny<CancellationToken>())).Returns(4);
            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            var result = mongoStore.Count<DataEntity>();

            Assert.AreEqual(4, result);
        }

        [TestMethod]
        public void Count_WhenCalledWithCriteria_ReturnsCollectionCount()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});
            Expression<Func<DataEntity, bool>> criteria = arg => arg.Id.Equals(new Guid("F38B1979-E668-4367-B4B9-2FF8D40AAC07"));
            FilterDefinition<DataEntity> filterDefinition = criteria;

            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);
            mongoCollection.Setup(x =>
                x.Count(It.Is<FilterDefinition<DataEntity>>(e => RenderFilterToBsonDocument(e).ToString() == RenderFilterToBsonDocument(filterDefinition).ToString()),
                    It.IsAny<CountOptions>(), It.IsAny<CancellationToken>())).Returns(4);
            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            var result = mongoStore.Count(criteria);

            Assert.AreEqual(4, result);
        }

        [TestMethod]
        public void Distinct_WhenCalled_CallsDistinct()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});
            Expression<Func<DataEntity, bool>> criteria = arg => true;
            FilterDefinition<DataEntity> filterDefinition = criteria;

            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);
            var indexCalled = false;
            var asyncCursorGuid = new Mock<IAsyncCursor<object>>();
            asyncCursorGuid.Setup(x => x.MoveNext(It.IsAny<CancellationToken>())).Returns(() =>
            {
                var localIndexCalled = !indexCalled;
                indexCalled = true;
                return localIndexCalled;
            });
            asyncCursorGuid.SetupGet(x => x.Current).Returns(() => new List<object>
            {
                Guid.NewGuid()
            });

            mongoCollection.Setup(x => x.Distinct(
                    It.Is<FieldDefinition<DataEntity, object>>(e => RenderFieldToBsonDocument(e).FieldName == "_id"),
                    It.Is<FilterDefinition<DataEntity>>(e => RenderFilterToBsonDocument(e).ToString() == RenderFilterToBsonDocument(filterDefinition).ToString()),
                    It.IsAny<DistinctOptions>(),
                    It.IsAny<CancellationToken>()
                ))
                .Returns(asyncCursorGuid.Object);
            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            var result = mongoStore.Distinct(arg => arg.Id, filterDefinition);

            Assert.AreNotEqual(0, result.Count());
            mongoCollection.Verify(
                x => x.Distinct(It.IsAny<FieldDefinition<DataEntity, object>>(), It.IsAny<FilterDefinition<DataEntity>>(), It.IsAny<DistinctOptions>(),
                    It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public void Put_WhenItemDoesNotExist_CallsInsertOne()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});

            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);
            mongoCollection.Setup(x => x.FindOneAndReplace(It.IsAny<FilterDefinition<DataEntity>>(), It.IsAny<DataEntity>(),
                It.IsAny<FindOneAndReplaceOptions<DataEntity, DataEntity>>(), It.IsAny<CancellationToken>())).Returns(default(DataEntity));

            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            mongoStore.Put(new Mock<DataEntity>().Object);

            mongoCollection.Verify(
                x => x.FindOneAndReplace(It.IsAny<FilterDefinition<DataEntity>>(), It.IsAny<DataEntity>(), It.IsAny<FindOneAndReplaceOptions<DataEntity, DataEntity>>(),
                    It.IsAny<CancellationToken>()), Times.Once);
            mongoCollection.Verify(x => x.InsertOne(It.IsAny<DataEntity>(), It.IsAny<InsertOneOptions>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public void Put_WhenItemDoesExist_DoesNotCallInsertOne()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});

            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);
            mongoCollection.Setup(x => x.FindOneAndReplace(It.IsAny<FilterDefinition<DataEntity>>(), It.IsAny<DataEntity>(),
                It.IsAny<FindOneAndReplaceOptions<DataEntity, DataEntity>>(), It.IsAny<CancellationToken>())).Returns(new Mock<DataEntity>().Object);

            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            mongoStore.Put(new Mock<DataEntity>().Object);

            mongoCollection.Verify(
                x => x.FindOneAndReplace(It.IsAny<FilterDefinition<DataEntity>>(), It.IsAny<DataEntity>(), It.IsAny<FindOneAndReplaceOptions<DataEntity, DataEntity>>(),
                    It.IsAny<CancellationToken>()), Times.Once);
            mongoCollection.Verify(x => x.InsertOne(It.IsAny<DataEntity>(), It.IsAny<InsertOneOptions>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [TestMethod]
        public void Put_WithCriteria_CallsDeleteManyAndInsertOne()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});
            Expression<Func<DataEntity, bool>> criteria = arg => arg.Id.Equals(new Guid("F38B1979-E668-4367-B4B9-2FF8D40AAC07"));
            FilterDefinition<DataEntity> filterDefinition = criteria;

            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);

            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            mongoStore.Put(new Mock<DataEntity>().Object, criteria);

            mongoCollection.Verify(
                x => x.DeleteMany(
                    It.Is<FilterDefinition<DataEntity>>(e => RenderFilterToBsonDocument(e).ToString() == RenderFilterToBsonDocument(filterDefinition).ToString()),
                    It.IsAny<DeleteOptions>(), It.IsAny<CancellationToken>()), Times.Once);
            mongoCollection.Verify(x => x.InsertOne(It.IsAny<DataEntity>(), It.IsAny<InsertOneOptions>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public void Update_WithCriteria_CallsDeleteManyAndInsertOne()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});
            Expression<Func<DataEntity, bool>> criteria = arg => arg.Id.Equals(new Guid("F38B1979-E668-4367-B4B9-2FF8D40AAC07"));
            FilterDefinition<DataEntity> filterDefinition = criteria;
            UpdateDefinition<DataEntity> updateDefinition = Builders<DataEntity>.Update.Set("Id", new Guid("F38B1979-E668-4367-B4B9-2FF8D40AAC08"));

            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);

            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            mongoStore.Update(criteria, updateDefinition);

            mongoCollection.Verify(
                x => x.UpdateMany(
                    It.Is<FilterDefinition<DataEntity>>(e => RenderFilterToBsonDocument(e).ToString() == RenderFilterToBsonDocument(filterDefinition).ToString()),
                    It.IsAny<UpdateDefinition<DataEntity>>(), It.IsAny<UpdateOptions>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public void Remove_WithExpression_CallsDeleteMany()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});
            Expression<Func<DataEntity, bool>> criteria = arg => arg.Id.Equals(new Guid("F38B1979-E668-4367-B4B9-2FF8D40AAC07"));
            FilterDefinition<DataEntity> filterDefinition = criteria;

            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);

            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            mongoStore.Remove(criteria);

            mongoCollection.Verify(
                x => x.DeleteMany(
                    It.Is<FilterDefinition<DataEntity>>(e => RenderFilterToBsonDocument(e).ToString() == RenderFilterToBsonDocument(filterDefinition).ToString()),
                    It.IsAny<DeleteOptions>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public void DropCollection_WithType_CallsDropCollectionForCorrectType()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});

            var mongoDatabase = new Mock<IMongoDatabase>();
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            mongoStore.DropCollection<DataEntity>();

            mongoDatabase.Verify(x => x.DropCollection(typeof(DataEntity).Name, It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public void Get_WithGuid_CallsFindSync()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(arg => arg.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});
            var guid = new Guid("F38B1979-E668-4367-B4B9-2FF8D40AAC07");
            Expression<Func<DataEntity, bool>> criteria = o => o.Id.Equals(guid);
            FilterDefinition<DataEntity> filterDefinition = criteria;

            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);
            mongoCollection.Setup(x => x.FindSync(It.IsAny<FilterDefinition<DataEntity>>(), It.IsAny<FindOptions<DataEntity, DataEntity>>(), It.IsAny<CancellationToken>()))
                .Returns(new Mock<IAsyncCursor<DataEntity>>().Object);

            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            mongoStore.Get<DataEntity>(guid);

            mongoCollection.Verify(
                x => x.FindSync(It.Is<FilterDefinition<DataEntity>>(e => RenderFilterToBsonDocument(e).ToString() == RenderFilterToBsonDocument(filterDefinition).ToString()),
                    It.IsAny<FindOptions<DataEntity, DataEntity>>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public void Get_WithExpression_CallsFindSync()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});
            Expression<Func<DataEntity, bool>> criteria = arg => arg.Id.Equals(new Guid("F38B1979-E668-4367-B4B9-2FF8D40AAC07"));
            FilterDefinition<DataEntity> filterDefinition = criteria;

            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);

            var indexCalled = false;
            var asyncCursorGuid = new Mock<IAsyncCursor<DataEntity>>();
            asyncCursorGuid.Setup(x => x.MoveNext(It.IsAny<CancellationToken>())).Returns(() =>
            {
                var localIndexCalled = !indexCalled;
                indexCalled = true;
                return localIndexCalled;
            });
            asyncCursorGuid.SetupGet(x => x.Current).Returns(() => new List<DataEntity>
            {
                new Mock<DataEntity>().Object
            });
            mongoCollection.Setup(x => x.FindSync(It.IsAny<FilterDefinition<DataEntity>>(), It.IsAny<FindOptions<DataEntity, DataEntity>>(), It.IsAny<CancellationToken>()))
                .Returns(asyncCursorGuid.Object);

            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            var result = mongoStore.Get(criteria);

            Assert.AreNotEqual(0, result.Count());
            mongoCollection.Verify(
                x => x.FindSync(It.Is<FilterDefinition<DataEntity>>(e => RenderFilterToBsonDocument(e).ToString() == RenderFilterToBsonDocument(filterDefinition).ToString()),
                    It.IsAny<FindOptions<DataEntity, DataEntity>>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public void GetList_WithExpression_CallsFindSync()
        {
            var logger = new Mock<ILogger>();
            var configurationManager = new Mock<IConfigurationManager>();
            configurationManager.SetupGet(x => x.AppSettings).Returns(new AutoDictionary<string, string> {{"MongoDatabaseUri", ""}, {"MongoDatabaseName", ""}});
            Expression<Func<DataEntity, bool>> criteria = arg => arg.Id.Equals(new Guid("F38B1979-E668-4367-B4B9-2FF8D40AAC07"));
            FilterDefinition<DataEntity> filterDefinition = criteria;

            var asyncCursor = new Mock<IAsyncCursor<BsonDocument>>();
            var mongoIndexManager = new Mock<IMongoIndexManager<DataEntity>>();
            mongoIndexManager.Setup(x => x.List(It.IsAny<CancellationToken>())).Returns(asyncCursor.Object);
            var mongoCollection = new Mock<IMongoCollection<DataEntity>>();
            mongoCollection.SetupGet(x => x.Indexes).Returns(mongoIndexManager.Object);

            var indexCalled = false;
            var asyncCursorGuid = new Mock<IAsyncCursor<DataEntity>>();
            asyncCursorGuid.Setup(x => x.MoveNext(It.IsAny<CancellationToken>())).Returns(() =>
            {
                var localIndexCalled = !indexCalled;
                indexCalled = true;
                return localIndexCalled;
            });
            asyncCursorGuid.SetupGet(x => x.Current).Returns(() => new List<DataEntity>
            {
                new Mock<DataEntity>().Object
            });
            mongoCollection.Setup(x => x.FindSync(It.IsAny<FilterDefinition<DataEntity>>(), It.IsAny<FindOptions<DataEntity, DataEntity>>(), It.IsAny<CancellationToken>()))
                .Returns(asyncCursorGuid.Object);

            var mongoDatabase = new Mock<IMongoDatabase>();
            mongoDatabase.Setup(x => x.GetCollection<DataEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(mongoCollection.Object);
            _mongoClient.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(mongoDatabase.Object);

            var mongoStore = Sut;
            mongoStore.ClearCache<DataEntity>();

            var result = mongoStore.GetList(criteria);

            Assert.AreNotEqual(0, result.Count());
            mongoCollection.Verify(
                x => x.FindSync(It.Is<FilterDefinition<DataEntity>>(e => RenderFilterToBsonDocument(e).ToString() == RenderFilterToBsonDocument(filterDefinition).ToString()),
                    It.IsAny<FindOptions<DataEntity, DataEntity>>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public void ContainerCanConstructMongoStore()
        {
            var container = new Container(new MicroserviceRegistry(typeof(MongoStoreTests).Assembly));
            var result = container.GetInstance<IMongoStore>();
            Assert.AreEqual(typeof(MongoStore), result.GetType());
        }

        private static BsonDocument RenderFilterToBsonDocument<T>(FilterDefinition<T> filter)
        {
            var serializerRegistry = BsonSerializer.SerializerRegistry;
            var documentSerializer = serializerRegistry.GetSerializer<T>();
            return filter.Render(documentSerializer, serializerRegistry);
        }

        private static RenderedFieldDefinition<TField> RenderFieldToBsonDocument<TDocument, TField>(FieldDefinition<TDocument, TField> field)
        {
            var serializerRegistry = BsonSerializer.SerializerRegistry;
            var documentSerializer = serializerRegistry.GetSerializer<TDocument>();
            return field.Render(documentSerializer, serializerRegistry);
        }
    }
}