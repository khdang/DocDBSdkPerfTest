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
            var endRange = startRange * 15;
            var interval = DocDBHelper.PartitionCount * 5;
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
                    var throttlePercentage = totalRequests > 0 ? 1.0 * SimpleThroughputHelper.GetInstance().ThrottleCount / totalRequests : 0;
                    var avgThroughput = 1000.0 * SimpleThroughputHelper.GetInstance().TotalCompletion * ThroughputHelper.RUPerCycle / SimpleThroughputHelper.GetInstance().RuntimeMs;
                    var clientThroughput = SimpleThroughputHelper.GetInstance().GetAvgThroughput();

                    Console.WriteLine("\tAvg. RUs: {0:n1}, Client avg. RUs: {1:n1}, Req: {2:n0}, Throttle: {3:p1}, Runtime: {4:n0}ms",
                        avgThroughput,
                        clientThroughput,
                        totalRequests,
                        throttlePercentage,
                        SimpleThroughputHelper.GetInstance().RuntimeMs);
                }
            }
        }

        public void Run()
        {
            Console.WriteLine("\nRunning with fixed threads. ThreadCount={0}", ThreadCount);
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
