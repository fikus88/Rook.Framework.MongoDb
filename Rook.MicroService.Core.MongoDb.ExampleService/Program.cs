using System;
using System.Threading;
using Rook.MicroService.Core.Common;
using Rook.MicroService.Core.IoC;
using Rook.MicroService.Core.Services;
using Rook.MicroService.Core.StructureMap;

namespace Rook.MicroService.Core.MongoDb.ExampleService
{
    class Program
    {
        static void Main(string[] args)
        {
            // ---------------------------------
            //  STRUCTURE MAP CONFIG // If you're using Structuremap - i.e. UseStructureMap=true in config.json
            // ---------------------------------
            var container = Bootstrapper.Init();

            var whatDidIScan = container.WhatDidIScan();
            var whatDoIHave = container.WhatDoIHave();

            var instance = container.GetInstance<IService>();

            // It's also possible to use these utility methods so we can see what the container has scanned/registered
            // var whatDidIScan = container.WhatDidIScan();
            // var whatDoIHave = container.WhatDoIHave();
            // ---------------------------------

            // ---------------------------------
            //  LEGACY CONFIG // If you're not using Structuremap - you probably should - but you need to use this
            // ---------------------------------
            //Container.Map<IContainerFacade>(new ContainerFacade(null, Container.GetInstance<IConfigurationManager>()));
            //var instance = Container.GetInstance<IService>();
            // ---------------------------------

            Thread.CurrentThread.Name = $"{ServiceInfo.Name} Main Thread";
            instance.Start();

            AppDomain.CurrentDomain.ProcessExit += (s, e) => instance.Stop();

            Thread.CurrentThread.IsBackground = true;

            Container.GetInstance<ILogger>().Info($"{nameof(Program)}.{nameof(Main)}",
                new LogItem("Event", "Service started successfully"));

            while (true)
                Thread.Sleep(int.MaxValue);
        }
    }
}
