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
    class RunWithThreads : IPerfRunner
    {
        private static int threadCount = 0;
        private static int MaxThreads = 200;
        private static int RunningTimeSecs = 30;

        public void RunWithDiffferentSettings()
        {
            var startRange = Environment.ProcessorCount;
            var endRange = 1024;
            var interval = Environment.ProcessorCount * 4;
            for (int i = startRange; i <= endRange; i += interval)
            {
                ThroughputHelper.ResetStats();

                MaxThreads = i;
                Run();

                Console.WriteLine("Average throughput: {0:0}", ThroughputHelper.ThroughputStats.Mean);
            }
        }

        public void Run()
        {
            Console.WriteLine("Running with threads. MaxThreads={0}, RunningTimeSecs={1}", MaxThreads, RunningTimeSecs);

            var sw = Stopwatch.StartNew();
            var threads = new List<Thread>();

            while (sw.ElapsedMilliseconds/1000 < RunningTimeSecs)
            {
                if (ThroughputHelper.GetInstance().IsUnderOfferThroughput())
                {
                    for (int i = 1; i <= MaxThreads - threadCount; ++i)
                    {
                        var ts = new ThreadStart(() =>
                        {
                            DocDBHelper.RunOneCycle().Wait();
                            Interlocked.Decrement(ref threadCount);
                        });
                        var t = new Thread(ts);
                        threads.Add(t);
                        t.Start();
                        Interlocked.Increment(ref threadCount);
                    }
                    //Console.WriteLine("Queue {0} work items", WorkItemCreationRate);
                }

                Thread.Sleep(Program.UpdateFreq);
            }

            for (int i = 0; i < threads.Count; ++i)
            {
                threads[i].Join();
            }
        }
    }
}
