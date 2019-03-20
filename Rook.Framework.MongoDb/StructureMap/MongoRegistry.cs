using System.Reflection;
using Rook.Framework.MongoDb.Data;
using Rook.Framework.Core.Services;
using StructureMap;

namespace Rook.Framework.MongoDb.StructureMap
{
    public class MongoRegistry : Registry
    {
        public MongoRegistry() : this(Assembly.GetEntryAssembly()) { }

        public MongoRegistry(Assembly assmebly)
        {
            
            Scan(scan =>
            {
                scan.Assembly(assmebly);
                scan.AddAllTypesOf<DataEntityBase>();
            });

            For<IStartable>().Add<MongoStore>();

            For<IMongoClient>().ClearAll().Singleton().Use<MongoClient>();
            For<IMongoStore>().ClearAll().Singleton().Use<MongoStore>();
        }
    }
}
