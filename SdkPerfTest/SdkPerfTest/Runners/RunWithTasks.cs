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
    class RunWithTasks
    {
        private const int NumberOfTasks = 100000;
        private static List<Task> tasks = new List<Task>();

        public async Task Run()
        {
            Console.WriteLine("Running with Task.WhenAll(). NumberOfTask={0}", NumberOfTasks);

            for (int i = 0; i < NumberOfTasks; ++i)
            {
                tasks.Add(RunOneCycle());
            }
            await Task.WhenAll(tasks.ToArray());
        }

        private static async Task RunOneCycle()
        {
            await DocDBHelper.RunOneCycle();
        }
    }
}
