using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SdkPerfTest
{
    class ThroughputHelper
    {
        private static ThroughputHelper _instance = new ThroughputHelper();
        private static readonly Queue<ResponseEntry> responseQueue = new Queue<ResponseEntry>();
        private static object updateLock = new object();

        private static Stopwatch stopwatch = null;
        private const int warmupSecs = 10;
        private static bool hasWarmedup = false;
        
        public static StatsTracker ThroughputStats;
        public static StatsTracker RequestStats;

        public static double Throughput = 0;
        public static int RequestCount = 0;
        public static int ThrottleCount = 0;
        public static long RUPerCycle;

        static ThroughputHelper() {
            ResetStats();
        }

        public static void ResetStats()
        {
            responseQueue.Clear();

            ThroughputStats = new StatsTracker() { StatName = "Throughput" };
            RequestStats = new StatsTracker() { StatName = "Req/sec" };

            Throughput = 0;
            RequestCount = 0;
            ThrottleCount = 0;

            stopwatch = Stopwatch.StartNew();
            hasWarmedup = false;
        }

        public static ThroughputHelper GetInstance()
        {
            return _instance;
        }

        public bool IsUnderOfferThroughput()
        {
            return Throughput < DocDBHelper.OfferThroughput;
        }

        public void RequestComplete(double throughput, int statusCode)
        {
            lock (updateLock)
            {
                Throughput += throughput;
                RequestCount++;
                //Console.WriteLine("Status code: " + statusCode.ToString());
                if (statusCode != 200 && statusCode != 201)
                {
                    ThrottleCount++;
                }
                responseQueue.Enqueue(new ResponseEntry(DateTime.Now.Ticks, throughput, statusCode));
            }
        }

        public void UpdateStats()
        {
            long currentTicks = DateTime.Now.Ticks;
            ResponseEntry response = null;
            lock (updateLock)
            {
                while (responseQueue.Count > 0 && currentTicks - responseQueue.Peek().Ticks > 10000000)
                {
                    response = responseQueue.Dequeue();
                    Throughput -= response.Throughput;
                    RequestCount--;
                    if (response.StatusCode != 200 && response.StatusCode != 201)
                    {
                        ThrottleCount--;
                    }
                }
                UpdateAverage();
            }
        }

        private void UpdateAverage()
        {
            if (stopwatch.ElapsedMilliseconds / 1000 >= warmupSecs)
            {
                stopwatch.Stop();
                hasWarmedup = true;
            }
            if (Throughput > 0 && RequestCount > 0 && hasWarmedup)
            {
                ThroughputStats.AddEntry(Throughput);
                RequestStats.AddEntry(RequestCount);
            }
        }

        public void PrintStatus()
        {
            Console.WriteLine(ToString());   
        }

        public override string ToString()
        {
            return string.Format("Throughtput: {0,5:0} avg {1,5:0}\tReq/sec: {2,5} avg {3,5:0}\tThrottleCount: {4,5}", Throughput, ThroughputStats.Mean, RequestCount, RequestStats.Mean, ThrottleCount);
        }
    }

    class ResponseEntry
    {
        private long _ticks;
        public long Ticks { get { return _ticks; } }

        private double _throughput;
        public double Throughput { get { return _throughput; } }

        private int _statusCode;
        public int StatusCode { get { return _statusCode; } }

        public ResponseEntry(long ticks, double throughput, int statusCode)
        {
            _ticks = ticks;
            _throughput = throughput;
            _statusCode = statusCode;
        }
    }

    class StatsTracker
    {
        private long count = 0;
        private double total = 0;
        private double min = double.MaxValue;
        private double max = 0;
        private double mean = 0;

        public string StatName { get; set; }

        public void AddEntry(double val)
        {
            count++;
            total += val;
            if (min > val)
            {
                min = val;
            }
            if (max < val)
            {
                max = val;
            }
        }

        public double Mean
        {
            get 
            { 
                mean = count == 0 ? 0 : total / count;
                return mean; 
            }
        }
    }
}
