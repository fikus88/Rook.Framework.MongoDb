using MongoDB.Bson;
using MongoDB.Driver;

namespace Rook.Framework.MongoDb.Data {
	public class MongoClient : IMongoClient
	{
		private MongoDB.Driver.MongoClient _mongoClient;

		public void Create(string connectionString)
		{
            MongoDefaults.GuidRepresentation = GuidRepresentation.Standard;

            _mongoClient = new MongoDB.Driver.MongoClient(connectionString);
		}

		public IMongoDatabase GetDatabase(string name, MongoDatabaseSettings settings = null)
		{
			if (_mongoClient == null) throw new MongoClientException($"{nameof(MongoClient)}.{nameof(Create)} must be run before calling {nameof(GetDatabase)}");

			return _mongoClient.GetDatabase(name, settings);
		}
	}
}