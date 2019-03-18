using Rook.MicroService.Core.Application.Message;
using Rook.MicroService.Core.Application.MessageHandlers;
using Rook.MicroService.Core.Attributes;
using Rook.MicroService.Core.MongoDb.Data;

namespace Rook.MicroService.Core.MongoDb.ExampleService
{
    [Handler("MongoTest", AcceptanceBehaviour = AcceptanceBehaviour.OnlyWithoutSolution)]
    public class MongoAccess : IMessageHandler2<int, long>
    {
        private readonly IMongoStore _mongoStore;

        public MongoAccess(IMongoStore mongoStore)
        {
            _mongoStore = mongoStore;
        }

        public CompletionAction Handle(Message<int, long> message)
        {
            var newDto = new Dto {I = message.Need};
            _mongoStore.Put(newDto);

            message.Solution = new []{ _mongoStore.Count<Dto>() };
            return CompletionAction.Republish;
        }
    }

    public class Dto : DataEntity
    {
        public int I { get; set; }
    }
}
