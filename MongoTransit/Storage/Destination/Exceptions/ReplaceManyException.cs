using System;
using System.Collections.Generic;

namespace MongoTransit.Storage.Destination.Exceptions
{
    public class ReplaceManyException: Exception
    {
        public List<ReplaceErrorInfo> Errors { get; }
        public int ProcessedCount { get; }

        public ReplaceManyException(List<ReplaceErrorInfo> errors, int processedCount)
        {
            Errors = errors;
            ProcessedCount = processedCount;
        }
        
        public ReplaceManyException(Exception inner): base("Replace many error", inner)
        {
            Errors = new List<ReplaceErrorInfo>();
        }
    }
}