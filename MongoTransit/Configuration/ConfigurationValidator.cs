using System.Collections.Generic;
using System.Linq;

namespace MongoTransit.Configuration;

public static class ConfigurationValidator
{
    public static IEnumerable<string> Validate(IReadOnlyList<ConfigurationReader.CollectionOption> collectionOptions)
    {
        for (var i = 0; i < collectionOptions.Count; i++)
        {
            var option = collectionOptions[i];
            if (string.IsNullOrWhiteSpace(option.Database))
                yield return $"[{i}] {nameof(option.Database)} should not be empty";

            if (string.IsNullOrWhiteSpace(option.Name))
                yield return $"[{i}] {nameof(option.Name)} should not be empty";

            if (option.KeyFields?.Any(string.IsNullOrWhiteSpace) == true)
                yield return $"[{i}] {nameof(option.KeyFields)} should not contain empty values";
            
            if (option.IterativeOptions == null)
                continue;

            var iter = option.IterativeOptions;

            if (string.IsNullOrWhiteSpace(iter.Field))
                yield return $"[{i}.{nameof(option.IterativeOptions)}] {nameof(iter.Field)} should not be empty";

            if (iter.OffsetInMinutes < 0)
                yield return
                    $"[{i}.{nameof(option.IterativeOptions)}] {nameof(iter.OffsetInMinutes)} should greater or equal to zero (current value is {iter.OffsetInMinutes})";
        }
    }
}