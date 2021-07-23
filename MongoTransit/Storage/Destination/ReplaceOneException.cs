using System;

namespace MongoTransit.Storage.Destination
{
    public class ReplaceOneException: Exception
    {
        public ReplaceOneException(string message, Exception inner): base(message, inner)
        {
        }
    }
}