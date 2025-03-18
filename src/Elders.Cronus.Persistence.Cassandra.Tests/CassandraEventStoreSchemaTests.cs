namespace Elders.Cronus.Persistence.Cassandra.Tests;

public class PartitionCalculatorTests
{
    [Theory]
    [InlineData(2024074, 2024075)] // March 14, 2024 -> March 15, 2024
    [InlineData(2024075, 2024076)] // March 15, 2024 -> March 16, 2024
    [InlineData(2023364, 2023365)] // Dec 30, 2023 -> Dec 31, 2023 (Non-leap year)
    [InlineData(2023365, 2024001)] // Dec 31, 2023 -> Jan 1, 2024 (Year transition)
    [InlineData(2024365, 2024366)] // Dec 30, 2024 -> Dec 31, 2024 (Leap year)
    [InlineData(2024366, 2025001)] // Dec 31, 2024 (Leap year) -> Jan 1, 2025 (Year transition)
    public void GetNext_ShouldReturnNextDayPartition(int currentPartition, int expectedNextPartition)
    {
        // Act
        int nextPartition = PartitionCalculator.GetNext(currentPartition);

        // Assert
        Assert.Equal(expectedNextPartition, nextPartition);
    }
}
