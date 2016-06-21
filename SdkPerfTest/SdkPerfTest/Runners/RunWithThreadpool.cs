using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Net;

namespace SdkPerfTest
{
    class RunWithThreadpool : IPerfRunner
    {
        private const int WorkItemCreationRate = 200;
        private const int RunningTimeSecs = 30;

        public void Run()
        {
            int workerThreads, ioThreads;
            ThreadPool.GetAvailableThreads(out workerThreads, out ioThreads);
            Console.WriteLine("Running with threadpool. WorkItemCreationRate={0}", WorkItemCreationRate);
            Console.WriteLine("Threadpool available workerThreads={0}, ioThreads={1}", workerThreads, ioThreads);

            var sw = Stopwatch.StartNew();

            while (sw.ElapsedMilliseconds/1000 < RunningTimeSecs)
            {
                if (ThroughputHelper.GetInstance().IsUnderOfferThroughput())
                {
                    for (int i = 0; i < WorkItemCreationRate; ++i)
                    {
                        ThreadPool.QueueUserWorkItem(RunOneCycle);
                    }
                    //Console.WriteLine("Queue {0} work items", WorkItemCreationRate);
                }

                Thread.Sleep(Program.UpdateFreq);
            }
        }

        private static void RunOneCycle(object state)
        {
            DocDBHelper.RunOneCycle();
        }
    }
}
