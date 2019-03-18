using System.Reflection;
using Rook.MicroService.Core.Services;
using Rook.MicroService.Core.MongoDb.Data;
using StructureMap;

namespace Rook.MicroService.Core.MongoDb.StructureMap
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
