using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SdkPerfTest.Helpers
{
    internal class SimpleThroughputHelper
    {
        public const int StopwatchDelaySecs = 10;

        public long TotalRequestCharge;
        public long ThrottleCount;
        public long TotalCompletion;
        public Stopwatch Watch;
        public long RuntimeMs;

        public long StartTotalRU;
        public long StartThrottleCount;
        public long StartTotalCompletion;

        public long DelayTotalRU;
        public long DelayThrottleCount;
        public long DelayTotalCompletion;

        private Timer delayTimer;
        private Timer startTimer;

        private static SimpleThroughputHelper _instance = new SimpleThroughputHelper();
        public static SimpleThroughputHelper GetInstance() {
            return _instance;
        }

        public void UpdateRunTime()
        {
            _instance.RuntimeMs = _instance.Watch.ElapsedMilliseconds;
        }

        private static void SaveDelayNumbers(object state)
        {
            _instance.DelayTotalRU = _instance.TotalRequestCharge;
            _instance.DelayTotalCompletion = _instance.TotalCompletion;
            _instance.DelayThrottleCount = _instance.ThrottleCount;
        }

        private static void SaveStartNumbers(object state)
        {
            _instance.StartTotalRU = _instance.TotalRequestCharge;
            _instance.StartTotalCompletion = _instance.TotalCompletion;
            _instance.StartThrottleCount = _instance.ThrottleCount;
        }

        internal static void Initialize()
        {
            _instance.Watch = Stopwatch.StartNew();
            _instance.TotalRequestCharge = 0;
            _instance.TotalCompletion = 0;
            _instance.ThrottleCount = 0;

            //_instance.delayTimer = new Timer(SaveDelayNumbers, null, 15000, Timeout.Infinite);
            //_instance.startTimer = new Timer(SaveStartNumbers, null, 5000, Timeout.Infinite);
        }

        public long GetTotalRU()
        {
            return _instance.DelayTotalRU - _instance.StartTotalRU;
        }

        public long GetThrottleCount()
        {
            return _instance.DelayThrottleCount - _instance.StartThrottleCount;
        }

        public long GetTotalCompletion()
        {
            return _instance.DelayTotalCompletion - _instance.StartTotalCompletion;
        }

        public void AddThrottle()
        {
            Interlocked.Increment(ref ThrottleCount);
        }

        public void AddEntry(long requestCharge)
        {
            Interlocked.Add(ref TotalRequestCharge, requestCharge);
            Interlocked.Increment(ref TotalCompletion);
        }

        public long GetAvgThroughput()
        {
            UpdateRunTime();
            //return TotalRequestCharge / RuntimeSecs;
            return GetTotalRU() / RuntimeMs;
        }
    }
}
