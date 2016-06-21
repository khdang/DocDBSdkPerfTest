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
    enum RunMode
    {
        Threadpool = 0,
        Tasks = 1,
        Threads = 2,
        FixedThreads = 3
    }

    class Program
    {
        internal static int UpdateFreq = 100;
        internal static int PrintStatusFreq = 1000;
        internal static bool RunWithDifferentDefaultConnLimit = false;
        internal static bool useThroughputHelper = false;
        internal static bool useSimpleThroughputHelper = true;

        internal static RunMode Mode = RunMode.Threads;

        private static ThroughputHelper throughputHelper = new ThroughputHelper();

        private static Timer updateStatusTimer;
        private static Timer printStatusTimer;

        static void Main(string[] args)
        {
            ServicePointManager.UseNagleAlgorithm = true;
            ServicePointManager.Expect100Continue = true;

            ReadConfig();

            ParseArgument(args);

            if (RunWithDifferentDefaultConnLimit)
            {
                var values = new int[] { 
                    50,
                    Environment.ProcessorCount * 100,
                    10000,
                    int.MaxValue
                };

                for (int i = 0; i < values.Length; ++i)
                {
                    ServicePointManager.DefaultConnectionLimit = values[i];

                    Console.WriteLine("\nRunning with ServicePointManager.DefaultConnectionLimit={0}", values[i]);

                    MainTask();

                    Console.WriteLine("Sleeping for 5secs..");
                    Thread.Sleep(5000);
                }
            }
            else
            {
                MainTask();
            }
        }

        private static void MainTask()
        {
            try
            {
                DocDBHelper.Initialize().Wait();

                long beforeRU = SimpleThroughputHelper.GetInstance().TotalRequestCharge;
                DocDBHelper.RunOneCycle().Wait();
                long afterRU = SimpleThroughputHelper.GetInstance().TotalRequestCharge;
                ThroughputHelper.RUPerCycle = afterRU - beforeRU;
                Console.WriteLine("RunOneCycle() RU: {0}", ThroughputHelper.RUPerCycle);

                if (useThroughputHelper)
                {
                    updateStatusTimer = new Timer(UpdateStatus, null, 1000, UpdateFreq);
                    printStatusTimer = new Timer(PrintStatus, null, 1000, PrintStatusFreq);
                }

                RunBenchmark();

                DocDBHelper.Cleanup();
            }
            catch (DocumentClientException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}, Message: {2}", de.StatusCode, de.Message, baseException.Message);
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
            finally
            {
                if (useThroughputHelper)
                {
                    updateStatusTimer.Change(0, Timeout.Infinite);
                    printStatusTimer.Change(0, Timeout.Infinite);
                }

                //Console.WriteLine("\nEnd of demo, press any key to exit.");
                //Console.ReadKey();
            }
        }

        private static void ReadConfig()
        {
            Console.WriteLine("\nReadConfig()");

            int tempInt;
            if (int.TryParse(ConfigurationManager.AppSettings["UpdateFreq"], out tempInt))
            {
                UpdateFreq = tempInt;
                Console.WriteLine("\tUpdateFreq = {0}", tempInt);
            }

            if (int.TryParse(ConfigurationManager.AppSettings["PrintStatusFreq"], out tempInt))
            {
                PrintStatusFreq = tempInt;
                Console.WriteLine("\tPrintStatusFreq = {0}", tempInt);
            }

            if (int.TryParse(ConfigurationManager.AppSettings["PartitionCount"], out tempInt))
            {
                DocDBHelper.PartitionCount = tempInt;
                DocDBHelper.OfferThroughput = 10000 * DocDBHelper.PartitionCount;
                Console.WriteLine("\tPartitionCount = {0}\r\n\tOfferThroughput = {1}", DocDBHelper.PartitionCount, DocDBHelper.OfferThroughput);
            }
        }

        private static void ParseArgument(string[] args)
        {
            Console.WriteLine("\nParseArgument()");

            int tempInt = 0;
            if (args.Length >= 1 && int.TryParse(args[0], out tempInt))
            {
                Mode = (RunMode)tempInt;
                Console.WriteLine("\tMode = {0}", Mode.ToString());
            }

            bool tempBool = false;
            if (args.Length >= 2 && bool.TryParse(args[1], out tempBool))
            {
                RunWithDifferentDefaultConnLimit = tempBool;
                Console.WriteLine("\tRunWithDifferentDefaultConnLimit = {0}", RunWithDifferentDefaultConnLimit);
            }

            if (args.Length >= 3 && bool.TryParse(args[2], out tempBool))
            {
                DocDBHelper.ReCreateDatabase = tempBool;
                Console.WriteLine("\tReCreateDatabase = {0}", tempBool);
            }
        }

        private static void RunBenchmark()
        {
            switch (Mode)
            {
                case RunMode.FixedThreads:
                    new RunWithFixedThreads().RunWithDiffferentSettings();
                    break;
                case RunMode.Tasks:
                    new RunWithTasks().Run().Wait();
                    break;
                case RunMode.Threadpool:
                    new RunWithThreadpool().Run();
                    break;
                case RunMode.Threads:
                    new RunWithThreads().RunWithDiffferentSettings();
                    break;
            }

            throughputHelper.PrintStatus();
        }

        private static void PrintStatus(object state)
        {
            throughputHelper.PrintStatus();
        }

        private static void UpdateStatus(object state)
        {
            throughputHelper.UpdateStats();
        }
    }
}
