using System;
using Rook.MicroService.Core.Common;
using Rook.MicroService.Core.Health;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;
using Rook.MicroService.Core.MongoDb.Data;

namespace Rook.MicroService.Core.MongoDb.Health
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
