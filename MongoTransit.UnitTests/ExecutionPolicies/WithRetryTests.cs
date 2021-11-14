using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FluentAssertions;
using MongoTransit.ExecutionPolicies;
using Moq;
using Xunit;

namespace MongoTransit.UnitTests.ExecutionPolicies;

public class WithRetryTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task ExecuteAsync_ShouldThrowArgumentException_AttemptsIsLessOrEqualToZero(int attempts)
    {
        // Arrange
        var func = () =>
            WithRetry.ExecuteAsync(attempts, TimeSpan.Zero, () => Task.FromResult(0), _ => true, default);

        // Act & Assert
        await func.Awaiting(f => f.Invoke()).Should().ThrowExactlyAsync<ArgumentException>();
    }
    
    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    public async Task ExecuteAsync_ShouldRunFunctionExactNumberOfTimes_WrappedFunctionThrowsException(int attempts)
    {
        // Arrange
        var count = 0;

        // Act
        try
        {
            await WithRetry.ExecuteAsync(attempts, TimeSpan.Zero,
                () => Task.FromException<int>(new Exception($"Attempt {count++} failed")), _ => true, default);
        }
        catch (Exception)
        {
            // Discard thrown exception
        }
        
        // Assert
        count.Should().Be(attempts);
    }
    
    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    public void ExecuteAsync_ShouldThrowExceptionFromFunction_WrappedFunctionThrowsException(int attempts)
    {
        // Arrange
        var exception = new Exception($"Attempt failed");
        var func = () => WithRetry.ExecuteAsync(attempts, TimeSpan.Zero, () => Task.FromException<int>(exception),
            _ => true, default);

        // Act & Assert
        func.Awaiting(f => f.Invoke()).Should().Throw<Exception>().Which.Should().Be(exception);
    }
    
    [Theory]
    [InlineData(2)]
    [InlineData(10)]
    public async Task ExecuteAsync_ShouldReturnValue_WrappedFunctionSucceededOnLastAttempt(int attempts)
    {
        // Arrange
        var wrappedFunc = new Mock<Func<Task<int>>>();
        var seq = wrappedFunc.SetupSequence(wf => wf());
        for (var i = 1; i < attempts; i++)
        {
            seq.Throws<Exception>();
        }
        seq.ReturnsAsync(42);

        // Act
        var actual = await WithRetry.ExecuteAsync(attempts, TimeSpan.Zero, wrappedFunc.Object, _ => true, default);
        
        // Assert
        actual.Should().Be(42);
    }
    
    [Fact]
    public async Task ExecuteAsync_ShouldWaitBeforeEachAttempt_WrappedFunctionSucceededOnLastAttempt()
    {
        // Arrange
        var cooldown = TimeSpan.FromSeconds(2);
        var waitBetweenAttempts = new Stopwatch();
        var wrappedFunc = () =>
        {
            if (waitBetweenAttempts.IsRunning)
            {
                waitBetweenAttempts.Stop();
                return Task.FromResult(42);
            }

            waitBetweenAttempts.Start();
            return Task.FromException<int>(new Exception("Attempt failed"));
        };

        // Act
        await WithRetry.ExecuteAsync(2, cooldown, wrappedFunc, _ => true, default);
        
        // Assert
        waitBetweenAttempts.Elapsed.Should().BeGreaterOrEqualTo(cooldown);
    }
    
    [Fact]
    public async Task ExecuteAsync_ShouldInvokeCallbackOnRetry_WrappedFunctionFails()
    {
        // Arrange
        var onRetryMock = new Mock<Action<Exception>>();
        var wrappedFunc = new Mock<Func<Task<int>>>();
        wrappedFunc.SetupSequence(wf => wf())
            .ThrowsAsync(new Exception())
            .ReturnsAsync(42);

        // Act
        await WithRetry.ExecuteAsync(2, TimeSpan.Zero, wrappedFunc.Object, _ => true, default, onRetryMock.Object);
        
        // Assert
        onRetryMock.Verify(a => a(It.IsAny<Exception>()), Times.Once);
    }
}