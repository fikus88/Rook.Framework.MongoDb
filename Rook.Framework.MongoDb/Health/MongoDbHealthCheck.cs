using System;
using Rook.Framework.Core.Common;
using Rook.Framework.Core.Health;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;
using Rook.Framework.MongoDb.Data;

namespace Rook.Framework.MongoDb.Health
{
    public class MongoDbHealthCheck : IHealthCheck
    {
        private readonly ILogger _logger;
        private readonly IMongoStore _mongoStore;

        public MongoDbHealthCheck(ILogger logger, IMongoStore mongoStore)
        {
            _logger = logger;
            _mongoStore = mongoStore;
        }

        public bool IsHealthy()
        {
            try
            {
                return _mongoStore.Ping();
            }
            catch (Exception ex)
            {
                _logger.Error($"{nameof(MongoDbHealthCheck)}.{nameof(IsHealthy)}",
                    new LogItem("Result", "Failed"),
                    new LogItem("Exception", ex.ToString));

                return false;
            }
        }
    }
}
