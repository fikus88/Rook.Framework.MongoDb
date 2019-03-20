using MongoDB.Driver;

namespace Rook.Framework.MongoDb.Data {
	public interface IMongoClient
	{
		void Create(string connectionString);
		IMongoDatabase GetDatabase(string name, MongoDatabaseSettings settings = null);
	}
}