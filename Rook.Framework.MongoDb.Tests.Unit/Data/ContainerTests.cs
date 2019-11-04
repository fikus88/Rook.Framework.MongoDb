using System.Linq;
using Rook.Framework.Core.StructureMap.Registries;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Rook.Framework.MongoDb.Data;
using StructureMap;

namespace Rook.Framework.MongoDb.Tests.Unit.Data
{
    [TestClass]
    public class ContainerTests
    {
        [TestMethod]
        public void Container_GetsAllDataEntityBaseClassesInEntryAssembly()
        {
            var container = new Container(new MicroserviceRegistry(typeof(ContainerTests).Assembly));
            var result = container.GetAllInstances<DataEntity>().ToList();

            var containsTestEntity = result.Any(x => x.GetType() == typeof(TestEntity));
            
            Assert.IsTrue(containsTestEntity);
        }
    }

    public class TestEntity : DataEntity
    {

    }


}
