﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Test
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;
    using DotNetty.Common.Concurrency;
    using Microsoft.Azure.Devices.Client.Common;
    using Microsoft.Azure.Devices.Client.Transport;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using NSubstitute;
    using NSubstitute.ExceptionExtensions;

    [TestClass]
    public class GateKeeperDelegatingHandlerTests
    {
        [TestMethod]
        [TestCategory("CIT")]
        [TestCategory("DelegatingHandlers")]
        [TestCategory("Owner [mtuchkov]")]
        public async Task OpenAsync_InnerCompleted_SutIsOpen()
        {
            var innerHandlerMock = Substitute.For<IDelegatingHandler>();
            innerHandlerMock.OpenAsync(true).Returns(t => TaskConstants.Completed);

            var sut = new GateKeeperDelegatingHandler(innerHandlerMock);
            await sut.OpenAsync();

            Assert.IsTrue(sut.Open);
            Assert.IsFalse(sut.Closed);
        }

        [TestMethod]
        [TestCategory("CIT")]
        [TestCategory("DelegatingHandlers")]
        [TestCategory("Owner [mtuchkov]")]
        public async Task ImplicitOpen_SubjWasNotOpen_SubjIsOpen()
        {
            var innerHandlerMock = Substitute.For<IDelegatingHandler>();
            innerHandlerMock.OpenAsync(false).Returns(t => TaskConstants.Completed);
            innerHandlerMock.SendEventAsync(Arg.Any<Message>()).Returns(t => TaskConstants.Completed);
            innerHandlerMock.SendEventAsync(Arg.Any<IEnumerable<Message>>()).Returns(t => TaskConstants.Completed);
            innerHandlerMock.ReceiveAsync().Returns(t => Task.FromResult(new Message()));
            innerHandlerMock.ReceiveAsync(Arg.Any<TimeSpan>()).Returns(t => Task.FromResult(new Message()));
            innerHandlerMock.AbandonAsync(Arg.Any<string>()).Returns(t => TaskConstants.Completed);
            innerHandlerMock.CompleteAsync(Arg.Any<string>()).Returns(t => TaskConstants.Completed);
            innerHandlerMock.RejectAsync(Arg.Any<string>()).Returns(t => TaskConstants.Completed);

            var actions = new Func<IDelegatingHandler, Task>[]
            {
                sut => sut.SendEventAsync(new Message()),
                sut => sut.SendEventAsync(new[] { new Message() }),
                sut => sut.ReceiveAsync(),
                sut => sut.ReceiveAsync(TimeSpan.FromSeconds(1)),
                sut => sut.AbandonAsync(string.Empty),
                sut => sut.CompleteAsync(string.Empty),
                sut => sut.RejectAsync(string.Empty),
            };

            foreach (Func<IDelegatingHandler, Task> action in actions)
            {
                var sut = new GateKeeperDelegatingHandler(innerHandlerMock);
                await action(sut);

                Assert.IsTrue(sut.Open);
                Assert.IsFalse(sut.Closed);
            }

            await innerHandlerMock.Received(actions.Length).OpenAsync(false);
        }

        [TestMethod]
        [TestCategory("CIT")]
        [TestCategory("DelegatingHandlers")]
        [TestCategory("Owner [mtuchkov]")]
        public async Task OpenAsync_ClosedCannotBeReopened_Throws()
        {
            var innerHandlerMock = Substitute.For<IDelegatingHandler>();
            innerHandlerMock.CloseAsync().Returns(t => TaskConstants.Completed);

            var sut = new GateKeeperDelegatingHandler(innerHandlerMock);
            await sut.CloseAsync();

            Assert.IsFalse(sut.Open);
            Assert.IsTrue(sut.Closed);

            await ((Func<Task>)sut.OpenAsync).ExpectedAsync<ObjectDisposedException>();
        }

        [TestMethod]
        [TestCategory("CIT")]
        [TestCategory("DelegatingHandlers")]
        [TestCategory("Owner [mtuchkov]")]
        public async Task OpenAsync_TwoCallers_OnlyOneOpenCalled()
        {
            var tcs = new TaskCompletionSource();
            var innerHandlerMock = Substitute.For<IDelegatingHandler>();
            int callCounter = 0;
            innerHandlerMock.OpenAsync(true).Returns(t =>
            {
                callCounter++;
                return tcs.Task;
            });

            var sut = new GateKeeperDelegatingHandler(innerHandlerMock);
            Task firstOpen = sut.OpenAsync();
            Task secondOpen = sut.OpenAsync();
            tcs.Complete();
            await Task.WhenAll(firstOpen, secondOpen);

            Assert.AreEqual(1, callCounter);
            Assert.IsTrue(sut.Open);
            Assert.IsFalse(sut.Closed);
        }

        [TestMethod]
        [TestCategory("CIT")]
        [TestCategory("DelegatingHandlers")]
        [TestCategory("Owner [mtuchkov]")]
        public async Task OpenAsync_InnerFailed_SutIsOpenAndCanBeReopen()
        {
            var innerHandlerMock = Substitute.For<IDelegatingHandler>();
            innerHandlerMock.OpenAsync(Arg.Is(true)).Returns(ci =>
            {
                throw new IOException();
            });
            var sut = new GateKeeperDelegatingHandler(innerHandlerMock);

            await ((Func<Task>)sut.OpenAsync).ExpectedAsync<IOException>();

            Assert.IsFalse(sut.Open);
            Assert.IsFalse(sut.Closed);

            innerHandlerMock.OpenAsync(Arg.Is(true)).Returns(t => TaskConstants.Completed);

            await sut.OpenAsync();
            Assert.IsTrue(sut.Open);
            Assert.IsFalse(sut.Closed);
        }

        [TestMethod]
        [TestCategory("CIT")]
        [TestCategory("DelegatingHandlers")]
        [TestCategory("Owner [mtuchkov]")]
        public async Task OpenAsync_InnerCancelled_SutIsOpenAndCanBeReopen()
        {
            var tcs = new TaskCompletionSource();
            tcs.SetCanceled();
            var innerHandlerMock = Substitute.For<IDelegatingHandler>();
            innerHandlerMock.OpenAsync(true).Returns(tcs.Task);
            var sut = new GateKeeperDelegatingHandler(innerHandlerMock);

            await sut.OpenAsync().ExpectedAsync<TaskCanceledException>();

            Assert.IsFalse(sut.Open);
            Assert.IsFalse(sut.Closed);

            innerHandlerMock.OpenAsync(true).Returns(t => TaskConstants.Completed);

            await sut.OpenAsync();
            Assert.IsTrue(sut.Open);
            Assert.IsFalse(sut.Closed);
        }
    }
}