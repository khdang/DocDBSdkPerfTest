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
using SdkPerfTest.Helpers;

namespace SdkPerfTest
{
    class RunWithFixedThreads : IPerfRunner
    {
        private static int ThreadCount = 200;
        private const int ThreadsTimeIteration = 1024;
        private const int MinIteration = 100;

        public void RunWithDiffferentSettings()
        {
            var startRange = DocDBHelper.PartitionCount * 5;
            var endRange = startRange * 10;
            var interval = startRange;
            for (int i = startRange; i <= endRange; i += interval)
            {
                if (Program.useThroughputHelper)
                {
                    ThroughputHelper.ResetStats();
                }
                if (Program.useSimpleThroughputHelper)
                {
                    SimpleThroughputHelper.Initialize();
                }

                ThreadCount = i;
                Run();

                if (Program.useThroughputHelper)
                {
                    Console.WriteLine("Average throughput: {0:0}", ThroughputHelper.ThroughputStats.Mean);
                }
                if (Program.useSimpleThroughputHelper)
                {
                    SimpleThroughputHelper.GetInstance().UpdateRunTime();
                    var totalRequests = SimpleThroughputHelper.GetInstance().TotalCompletion + SimpleThroughputHelper.GetInstance().ThrottleCount;
                    Console.WriteLine("Average throughput: {0:0}, TotalRequests: {1}, Throttle: {2:0.0%}, Total Run time: {3}ms",
                        1000.0 * SimpleThroughputHelper.GetInstance().TotalCompletion * ThroughputHelper.RUPerCycle / SimpleThroughputHelper.GetInstance().RuntimeMs,
                        totalRequests,
                        totalRequests > 0 ? SimpleThroughputHelper.GetInstance().ThrottleCount/totalRequests : 0,
                        SimpleThroughputHelper.GetInstance().RuntimeMs);
                }
            }
        }

        public void Run()
        {
            Console.WriteLine("Running with fixed threads. ThreadCount={0}", ThreadCount);
            int iterations = Math.Max(MinIteration, (int)Math.Ceiling(1.0*ThreadsTimeIteration/ThreadCount));

            var threads = new Thread[ThreadCount];
            for (int i = 0; i < ThreadCount; ++i)
            {
                var ts = new ThreadStart(() =>
                {
                    for (int j = 0; j < MinIteration; ++j)
                    {
                        try
                        {
                            DocDBHelper.RunOneCycle().Wait();
                        }
                        catch (AggregateException ex)
                        {
                            var dce = ex.InnerException as DocumentClientException;
                            if (dce != null)
                            {
                                if (Program.useSimpleThroughputHelper)
                                {
                                    SimpleThroughputHelper.GetInstance().AddThrottle();
                                }
                            }
                            else
                            {
                                throw ex;
                            }
                        }
                    }
                });
                threads[i] = new Thread(ts);
            }
            for (int i = 0; i < ThreadCount; ++i)
            {
                threads[i].Start();
            }
            for (int i = 0; i < ThreadCount; ++i)
            {
                threads[i].Join();
            }
        }
    }
}
