using System;

namespace Rook.MicroService.Core.MongoDb.Data {
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class MongoIndexAttribute : Attribute
    {
        public string IndexName;

        public MongoIndexAttribute(string indexName)
        {
            this.IndexName = indexName;
        }
    }
}