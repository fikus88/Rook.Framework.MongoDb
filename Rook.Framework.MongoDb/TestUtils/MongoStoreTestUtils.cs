using Rook.Framework.MongoDb.Data;
using Rook.Framework.Core.Common;

#pragma warning disable 618

namespace Rook.Framework.MongoDb.TestUtils
{
    public static class MongoStoreTestUtils
    {
        public static void DropCollection<T>(this MongoStore store) where T : DataEntityBase
        {
            store.Connect();
            store.Database.DropCollection(typeof(T).Name);
            store.Logger.Trace($"{nameof(MongoStore)}.{nameof(DropCollection)}",
                new LogItem("Event", "Drop collection"),
                new LogItem("Type", typeof(T).ToString));
        }

        /// <summary>
        /// Typically used with unit testing
        /// </summary>
        public static void ClearCache<T>(this MongoStore store)
        {
            MongoStore.CollectionCache.Remove(typeof(T));
        }
    }
}
