using System.Linq;
using Rook.MicroService.Core.StructureMap.Registries;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Rook.MicroService.Core.MongoDb.Data;
using StructureMap;

namespace Rook.MicroService.Core.MongoDb.Tests.Unit.Data
{
    [TestClass]
    public class ContainerTests
    {
        [TestMethod]
        public void Container_GetsAllDataEntityBaseClassesInEntryAssembly()
        {
            var container = new Container(new MicroserviceRegistry(typeof(ContainerTests).Assembly));
            var result = container.GetAllInstances<DataEntityBase>().ToList();

            var containsTestEntity = result.Any(x => x.GetType() == typeof(TestEntity));
            var containsTestObject = result.Any(x => x.GetType() == typeof(TestObject));

            Assert.IsTrue(containsTestEntity);
            Assert.IsTrue(containsTestObject);
        }
    }

    public class TestEntity : DataEntity
    {

    }

    public class TestObject : DataObject
    {

    }
}
