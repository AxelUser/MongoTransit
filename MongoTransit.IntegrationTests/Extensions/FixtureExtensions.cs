using System;
using AutoFixture;
using AutoFixture.Kernel;

namespace MongoTransit.IntegrationTests.Extensions
{
    public static class FixtureExtensions
    {
        public static DateTime CreateMongoDbDate(this ISpecimenBuilder builder)
        {
            var date = builder.Create<DateTime>();
            // Cut precision to milliseconds
            return date.AddTicks(-(date.Ticks % 10000));
        }
    }
}