using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.IdGenerators;
using Newtonsoft.Json;

#pragma warning disable 618

namespace Rook.Framework.MongoDb.Data
{
    public abstract class DataObjectIncrementalId : DataEntityBase
    {
        private static ulong number = (ushort)new Random().Next(0, 0xffff);

        protected DataObjectIncrementalId()
        {
            number = (ushort)(number + 1 & 0xffff);

            ulong n = (ulong)(DateTime.Now - new DateTime(2017, 12, 1)).TotalMilliseconds;
            n <<= 16;
            n += number;
            Sequence = n;
        }

        [BsonElement]
        [MongoIndex("Sequence")]
        public ulong Sequence { get; }

        [JsonConverter(typeof(ObjectIdConverter))]
        [BsonId(IdGenerator = typeof(ObjectIdGenerator))]
        public sealed override object Id { get; set; }
    }

    public abstract class DataObject : DataEntityBase
    {
        protected DataObject()
        {
            Id = ObjectId.GenerateNewId();
        }

        [JsonConverter(typeof(ObjectIdConverter))]
        [BsonId(IdGenerator = typeof(ObjectIdGenerator))]
        public sealed override object Id { get; set; }
    }

    public abstract class DataEntity : DataEntityBase
    {
        protected DataEntity()
        {
            Id = Guid.NewGuid();
        }

        [JsonConverter(typeof(GuidConverter))]
        [BsonId(IdGenerator = typeof(AscendingGuidGenerator))]
        public sealed override object Id { get; set; }
    }

    [Obsolete("Use DataEntity (for Guid IDs) or DataObject (for ObjectId IDs) or create your own implementation")]
    public abstract class DataEntityBase
    {
        [BsonIgnore]
        public abstract object Id { get; set; }

        [JsonIgnore]
        [BsonElement]
        public DateTime ExpiresAt { get; set; } = DateTime.UtcNow.AddMonths(18);

        [JsonIgnore]
        [BsonElement]
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }

    public class ObjectIdConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            serializer.Serialize(writer, value.ToString());
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (ObjectId.TryParse(serializer.Deserialize<string>(reader), out ObjectId objId))
                return objId;
            return null;
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(ObjectId);
        }
    }

    public class GuidConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            serializer.Serialize(writer, value);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return serializer.Deserialize<Guid>(reader);
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(Guid);
        }
    }
}