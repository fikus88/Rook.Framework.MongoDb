using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using Rook.Framework.Core.Common;
using Rook.Framework.Core.Services;
using Rook.Framework.Core.StructureMap;
using MongoDB.Bson;
using MongoDB.Driver;
using Rook.Framework.Core.AmazonKinesisFirehose;
using Rook.Framework.MongoDb.Helpers;
using JsonConvert = Newtonsoft.Json.JsonConvert;

#pragma warning disable 618

namespace Rook.Framework.MongoDb.Data
{
	public sealed class MongoStore : IMongoStore, IStartable
	{
		private readonly string _databaseName;

		internal readonly ILogger Logger;
		private readonly IMongoClient _client;
		private readonly IContainerFacade _containerFacade;
		internal IMongoDatabase Database;
		private readonly IAmazonFirehoseProducer _amazonFirehoseProducer;
		private readonly string _amazonKinesisStreamName;

		internal static Dictionary<Type, object> CollectionCache { get; } = new Dictionary<Type, object>();

		public MongoStore(
			ILogger logger,
			IConfigurationManager configurationManager,
			IMongoClient mongoClient,
			IContainerFacade containerFacade,
			IAmazonFirehoseProducer amazonFirehoseProducer)
		{
			var databaseUri = configurationManager.Get<string>("MongoDatabaseUri");
			_databaseName = configurationManager.Get<string>("MongoDatabaseName");
			
			try
			{
				_amazonKinesisStreamName = configurationManager.Get<string>("RepositoryKinesisStream");
			}
			catch
			{
				_amazonKinesisStreamName = null;
			}
			_client = mongoClient;
			_containerFacade = containerFacade;
			_client.Create(databaseUri);
			Logger = logger;
			if (!string.IsNullOrEmpty(_amazonKinesisStreamName))
				_amazonFirehoseProducer = new AmazonFirehoseProducer(logger);
		}

		public StartupPriority StartupPriority { get; } = StartupPriority.Highest;

		public void Start()
		{
			var dataEntities = _containerFacade.GetAllInstances<DataEntityBase>();
			foreach (var dataEntity in dataEntities)
			{
				var method = typeof(MongoStore).GetMethod(nameof(GetOrCreateCollection),
					BindingFlags.NonPublic | BindingFlags.Instance);
				method.MakeGenericMethod(dataEntity.GetType()).Invoke(this, new object[] { });
			}
		}

		internal void Connect()
		{
			if (Database == null)
				Database = _client.GetDatabase(_databaseName);
		}

		/// <summary>
		/// Gets an IQueryable collection of the DataEntity requested. If the collection does not already exist, it will be created.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		public IQueryable<T> QueryableCollection<T>() where T : DataEntityBase
		{
			Logger.Trace($"{nameof(MongoStore)}.{nameof(QueryableCollection)}",
				new LogItem("Event", "Get collection as queryable"), new LogItem("Type", typeof(T).ToString));
			return GetCollection<T>().AsQueryable();
		}

		/// <summary>
		/// Gets the Mongo collection of the DataEntity requested. If the collection does not already exist, it will be created.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		public IMongoCollection<T> GetCollection<T>() where T : DataEntityBase
		{
			lock (CollectionCache)
			{
				if (!CollectionCache.ContainsKey(typeof(T)))
				{
					Logger.Trace($"{nameof(MongoStore)}.{nameof(GetCollection)}<{typeof(T).Name}>",
						new LogItem("Action", "Not cached, call GetOrCreateCollection"));
					GetOrCreateCollection<T>();
				}

				return (IMongoCollection<T>) CollectionCache[typeof(T)];
			}
		}

		/// <summary>
		/// Returns the number of items of the requested type in the Mongo collection.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		public long Count<T>() where T : DataEntityBase
		{
			Logger.Trace($"{nameof(MongoStore)}.{nameof(Count)}",
				new LogItem("Event", "Get collection count"), new LogItem("Type", typeof(T).ToString));
			return GetCollection<T>().Count(arg => true);
		}

		/// <summary>
		/// Returns the number of items of the requested type in the Mongo collection filtered by the given expression.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		public long Count<T>(Expression<Func<T, bool>> expression, Collation collation = null) where T : DataEntityBase
		{
			CountOptions countOptions = new CountOptions() {Collation = collation};

			Logger.Trace($"{nameof(MongoStore)}.{nameof(Count)}",
				new LogItem("Event", "Get collection count"), new LogItem("Type", typeof(T).ToString),
				new LogItem("Expression", expression.ToString));
			return GetCollection<T>().Count(expression, countOptions);
		}

		public IEnumerable<TField> Distinct<TField, TCollection>(Expression<Func<TCollection, TField>> fieldSelector,
			FilterDefinition<TCollection> filterDefinition, Collation collation = null)
			where TCollection : DataEntityBase
		{
			IMongoCollection<TCollection> collection = GetCollection<TCollection>();

			Logger.Trace($"{nameof(MongoStore)}.{nameof(Distinct)}",
				new LogItem("Event", "Get distinct"),
				new LogItem("FieldType", typeof(TField).ToString),
				new LogItem("CollectionType", typeof(TCollection).ToString),
				new LogItem("FieldSelector", fieldSelector.ToString),
				new LogItem("FilterDefinition", filterDefinition.ToString));

			DistinctOptions distinctOptions = new DistinctOptions() {Collation = collation};

			using (IAsyncCursor<TField> cursor = collection.Distinct(fieldSelector, filterDefinition, distinctOptions))
				foreach (TField p in IterateCursor(cursor))
					yield return p;
		}

		private void GetOrCreateCollection<T>()
		{
			Connect();

			string collectionName = typeof(T).Name;

			Logger.Trace($"{nameof(MongoStore)}.{nameof(GetOrCreateCollection)}<{typeof(T).Name}>",
				new LogItem("Event", "Mongo GetCollection"));

			IMongoCollection<T> collection = Database.GetCollection<T>(collectionName);

			Dictionary<string, List<string>> indexes =
				new Dictionary<string, List<string>> {{"ExpiresAt", new List<string> {"ExpiresAt"}}};

			PropertyInfo[] members = typeof(T).GetProperties();
			foreach (PropertyInfo memberInfo in members)
			{
				MongoIndexAttribute indexAttribute = memberInfo.GetCustomAttribute<MongoIndexAttribute>();
				if (indexAttribute == null) continue;
				if (!indexes.ContainsKey(indexAttribute.IndexName))
					indexes.Add(indexAttribute.IndexName, new List<string>());
				indexes[indexAttribute.IndexName].Add(memberInfo.Name);
			}

			IMongoIndexManager<T> indexManager = collection.Indexes;
			foreach (KeyValuePair<string, List<string>> index in indexes)
			{
				bool indexExists = false;

				using (IAsyncCursor<BsonDocument> asyncCursor = indexManager.List())
					while (asyncCursor.MoveNext() && !indexExists)
					{
						indexExists = CheckIndexExists(asyncCursor, index);
					}

				if (!indexExists)
				{
					string indexJson = $"{{{string.Join(",", index.Value.Select(field => $"\"{field}\":1"))}}}";
					Logger.Trace($"{nameof(MongoStore)}.{nameof(GetOrCreateCollection)}<{typeof(T).Name}>",
						new LogItem("Action", $"Create ExpiresAt index"));

					CreateIndexOptions cio = new CreateIndexOptions {Name = index.Key};
					if (index.Key == "ExpiresAt") cio.ExpireAfter = TimeSpan.Zero;

					indexManager.CreateOne(new JsonIndexKeysDefinition<T>(indexJson), cio);
				}
			}

			CollectionCache.Add(typeof(T), collection);
		}

		private static bool CheckIndexExists(IAsyncCursor<BsonDocument> asyncCursor,
			KeyValuePair<string, List<string>> index)
		{
			bool indexExists = false;
			IEnumerable<BsonDocument> asyncCursorCurrent = asyncCursor.Current;
			foreach (BsonDocument bsonDocument in asyncCursorCurrent)
			{
				indexExists = bsonDocument["key"].AsBsonDocument.Elements
					.All(e => index.Value.Contains(e.Name));

				if (indexExists) break;
			}

			return indexExists;
		}

		/// <summary>
		/// Puts the given DataEntity into its corresponding Mongo collection
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="entityToStore"></param>
		public void Put<T>(T entityToStore) where T : DataEntityBase
		{
			IMongoCollection<T> collection = GetCollection<T>();

			if (collection.FindOneAndReplace(o => Equals(o.Id, entityToStore.Id), entityToStore) == null)
			{
				collection.InsertOne(entityToStore);

				
				if(!string.IsNullOrEmpty(_amazonKinesisStreamName))
					_amazonFirehoseProducer.PutRecord(_amazonKinesisStreamName,
					FormatEntity(entityToStore, OperationType.Insert));
				
				
				Logger.Trace($"{nameof(MongoStore)}.{nameof(Put)}",
					new LogItem("Event", "Insert entity"),
					new LogItem("Type", typeof(T).ToString),
					new LogItem("Entity", entityToStore.ToString));
			}
		}

		/// <summary>
		/// Replaces all items matching the filter with the given DataEntity in the corresponding Mongo collection
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="entityToStore"></param>
		/// <param name="filter"></param>
		/// <param name="collation"></param>
		public void Put<T>(T entityToStore, Expression<Func<T, bool>> filter, Collation collation = null)
			where T : DataEntityBase
		{
			DeleteOptions deleteOptions = new DeleteOptions {Collation = collation};

			IMongoCollection<T> collection = GetCollection<T>();
			var deleteResult = collection.DeleteMany(filter, deleteOptions);
			collection.InsertOne(entityToStore);

			
			if(!string.IsNullOrEmpty(_amazonKinesisStreamName))
				_amazonFirehoseProducer.PutRecord(_amazonKinesisStreamName,
				deleteResult.DeletedCount != 0
					? FormatEntity(entityToStore, OperationType.Update)
					: FormatEntity(entityToStore, OperationType.Insert));

			Logger.Trace($"{nameof(MongoStore)}.{nameof(Put)}",
				new LogItem("Event", "Insert entity"),
				new LogItem("Type", typeof(T).ToString),
				new LogItem("Entity", entityToStore.ToString),
				new LogItem("Filter", filter.Body.ToString));
		}

		public void Update<T>(Expression<Func<T, bool>> filter, UpdateDefinition<T> updates, Collation collation = null)
			where T : DataEntityBase
		{
			UpdateOptions updateOptions = new UpdateOptions() {Collation = collation};

			IMongoCollection<T> collection = GetCollection<T>();
			collection.UpdateMany(filter, updates, updateOptions);
			Logger.Trace($"{nameof(MongoStore)}.{nameof(Update)}",
				new LogItem("Event", "Update collection"),
				new LogItem("Type", typeof(T).ToString),
				new LogItem("Filter", filter.ToString),
				new LogItem("UpdateDefinition", updates.ToString));
		}

		public void Remove<T>(object id) where T : DataEntityBase
		{
			IMongoCollection<T> collection = GetCollection<T>();
			collection.DeleteOne(o => o.Id.Equals(id));
			Logger.Trace($"{nameof(MongoStore)}.{nameof(Remove)}",
				new LogItem("Event", "Remove entity"),
				new LogItem("Type", typeof(T).ToString),
				new LogItem("Id", id.ToString));
		}

		public void RemoveEntity<T>(T entityToRemove) where T : DataEntityBase
		{
			Remove<T>(entityToRemove.Id);
		}

		public void Remove<T>(Expression<Func<T, bool>> filter, Collation collation = null) where T : DataEntityBase
		{
			DeleteOptions deleteOptions = new DeleteOptions() {Collation = collation};

			IMongoCollection<T> collection = GetCollection<T>();
			collection.DeleteMany(filter, deleteOptions);
			Logger.Trace($"{nameof(MongoStore)}.{nameof(Remove)}",
				new LogItem("Event", "Remove entity"),
				new LogItem("Type", typeof(T).ToString),
				new LogItem("Filter", filter.ToString));
		}

		public T Get<T>(object id) where T : DataEntityBase
		{
			IMongoCollection<T> collection = GetCollection<T>();

			using (IAsyncCursor<T> cursor = collection.FindSync(o => Equals(id, o.Id)))
			{
				cursor.MoveNext();
				Logger.Trace($"{nameof(MongoStore)}.{nameof(Get)}",
					new LogItem("Event", "Get entity"),
					new LogItem("Type", typeof(T).ToString),
					new LogItem("Id", id.ToString));
				return cursor.Current.FirstOrDefault();
			}
		}

		public IEnumerable<T> Get<T>(Expression<Func<T, bool>> filter, Collation collation = null)
			where T : DataEntityBase
		{
			IMongoCollection<T> collection = GetCollection<T>();

			FindOptions<T, T> findOptions = new FindOptions<T, T> {Collation = collation};

			using (IAsyncCursor<T> cursor = collection.FindSync(filter, findOptions))
				foreach (T p in IterateCursor(cursor))
					yield return p;
		}

		public bool Ping()
		{
			Connect();
			return Database.RunCommandAsync((Command<BsonDocument>) "{ping:1}").Wait(1000);
		}

		private static IEnumerable<T> IterateCursor<T>(IAsyncCursor<T> cursor)
		{
			while (cursor != null && cursor.MoveNext())
				foreach (T obj in cursor.Current)
					yield return obj;
		}

		public IList<T> GetList<T>(Expression<Func<T, bool>> filter, Collation collation = null)
			where T : DataEntityBase
		{
			return Get(filter, collation).ToList();
		}

		private static string FormatEntity<T>(T entity, OperationType type)
		{
			var regex = new Regex("ISODate[(](.+?)[)]");

			var result = new
			{
				Service = ServiceInfo.Name,
				OperationType = Enum.GetName(typeof(OperationType), type),
				Entity = JsonConvert.SerializeObject(entity),
				EntityType = typeof(T).Name,
				Date = DateTime.UtcNow
			}.ToJson();

			return regex.Replace(result, "$1");
		}
	}
}