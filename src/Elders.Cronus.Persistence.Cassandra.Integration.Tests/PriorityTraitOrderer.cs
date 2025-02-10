using Xunit.Sdk;
using Xunit.v3;

namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

public class PriorityTraitOrderer : ITestCaseOrderer
{
    public IReadOnlyCollection<TTestCase> OrderTestCases<TTestCase>(IReadOnlyCollection<TTestCase> testCases) where TTestCase : notnull, ITestCase
    {
        var sortedMethods = new SortedDictionary<int, List<TTestCase>>();
        foreach (TTestCase testCase in testCases)
        {
            var traits = testCase.TestMethod.Traits
                .Where(x => x.Key == "priority")
                .SelectMany(x => x.Value)
                .Select(int.Parse);

            if (traits.Count() > 1)
                throw new InvalidOperationException("A test method can be decorated with only a single priority trait.");

            var priority = traits.First();
            GetOrCreate(sortedMethods, priority).Add(testCase);
        }

        var sortedByName = sortedMethods.Keys.SelectMany(priority => sortedMethods[priority].OrderBy(testCase => testCase.TestMethod.MethodName));
        return new List<TTestCase>(sortedByName);
    }

    private static TValue GetOrCreate<TKey, TValue>(IDictionary<TKey, TValue> dictionary, TKey key)
        where TKey : struct
        where TValue : new()
    {
        return dictionary.TryGetValue(key, out TValue result)
            ? result
            : (dictionary[key] = new TValue());
    }
}
