// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
namespace Microsoft.Azure.Devices.Client.Test
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client.Common;
    using Microsoft.Azure.Devices.Client.Exceptions;
    using Microsoft.Azure.Devices.Client.Transport;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using NSubstitute;
    using NSubstitute.ExceptionExtensions;

    [TestClass]
    public class ErrorDelegatingHandlerTests
    {
        [TestMethod]
        [TestCategory("CIT")]
        [TestCategory("DelegatingHandlers")]
        [TestCategory("Owner [mtuchkov]")]
        public async Task ErrorHandler_NoErrors_Success()
        {
            var innerHandler = Substitute.For<IDelegatingHandler>();
            innerHandler.OpenAsync(Arg.Is(false)).Returns(TaskConstants.Completed);
            innerHandler.SendEventAsync(Arg.Any<Message>()).Returns(TaskConstants.Completed);
            var sut = new ErrorDelegatingHandler(() => innerHandler);

            //emulate Gatekeeper behaviour
            await sut.OpenAsync(false);
            await sut.SendEventAsync(new Message(new byte[0]));

            await innerHandler.Received(1).OpenAsync(Arg.Is(false));
            await innerHandler.Received(1).SendEventAsync(Arg.Any<Message>());
        }

        [TestMethod]
        [TestCategory("CIT")]
        [TestCategory("DelegatingHandlers")]
        [TestCategory("Owner [mtuchkov]")]
        public async Task ErrorHandler_ChannelTransientErrorOccured_ChannelIsTheSame()
        {
            int ctorCallCounter = 0;
            var message = new Message(new byte[0]);
            var innerHandler = Substitute.For<IDelegatingHandler>();
            innerHandler.OpenAsync(Arg.Is(false)).Returns(TaskConstants.Completed);
            innerHandler.SendEventAsync(Arg.Is(message)).Throws<IotHubClientTransientException>();
            var sut = new ErrorDelegatingHandler(() =>
            {
                ctorCallCounter++;
                return innerHandler;
            });

            //initial OpenAsync to emulate Gatekeeper behaviour
            await sut.OpenAsync(false);
            await ((Func<Task>)(() => sut.SendEventAsync(message))).ExpectedAsync<IotHubClientTransientException>();

            innerHandler.SendEventAsync(Arg.Is(message)).Returns(TaskConstants.Completed);
            await sut.SendEventAsync(message);

            //assert
            await innerHandler.Received(1).OpenAsync(Arg.Is(false));
            await innerHandler.Received(2).SendEventAsync(Arg.Is(message));
            Assert.AreEqual(1, ctorCallCounter);
        }

        [TestMethod]
        [TestCategory("CIT")]
        [TestCategory("DelegatingHandlers")]
        [TestCategory("Owner [mtuchkov]")]
        public async Task ErrorHandler_TransientErrorOccured_ChannelIsRecreated()
        {
            int ctorCallCounter = 0;
            var message = new Message(new byte[0]);
            var innerHandler = Substitute.For<IDelegatingHandler>();
            innerHandler.OpenAsync(Arg.Is(false)).Returns(TaskConstants.Completed);
            innerHandler.SendEventAsync(Arg.Is(message)).Throws<TimeoutException>();
            var sut = new ErrorDelegatingHandler(() =>
            {
                ctorCallCounter++;
                return innerHandler;
            });

            //initial OpenAsync to emulate Gatekeeper behaviour
            await sut.OpenAsync(false);
            await ((Func<Task>)(() => sut.SendEventAsync(message))).ExpectedAsync<IotHubClientTransientException>();

            innerHandler.SendEventAsync(Arg.Is(message)).Returns(TaskConstants.Completed);
            await sut.SendEventAsync(message);

            //assert
            await innerHandler.Received(2).OpenAsync(Arg.Is(false));
            await innerHandler.Received(2).SendEventAsync(Arg.Is(message));
            Assert.AreEqual(2, ctorCallCounter);
        }
    }
}