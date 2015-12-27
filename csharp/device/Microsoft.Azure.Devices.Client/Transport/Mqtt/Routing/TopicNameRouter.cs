// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt.Routing
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics.Contracts;
    using System.Linq;
    using System.Text;
    using Microsoft.Azure.Devices.Client.Extensions;

    sealed class TopicNameRouter : ITopicNameRouter
    {
        const string DeviceIdPropertyName = "DeviceId";
        const string DeviceTemplateParameter = "{deviceId}";
        const string SingleSegmentWildcard = "+";
        const char KeyValueSeparator = '=';
        const char PropertySeparator = '&';
        const char SegmentSeparatorChar = '/';
        static readonly string SegmentSeparator = SegmentSeparatorChar.ToString();

        static readonly List<string> PropertiesToIgnore = new List<string>
        {
            DeviceIdPropertyName
        };

        //We assume that in avarage 20% of string are the encoded characters.
        //We can make a better estimation though.
        const float EncodedSymbolsFactor = 0.2f;

        string deviceBoundTopicFilter;
        string serviceBoundTopicFilterPrefix;
        string serviceBoundTopicFilterInfix;
        string serviceBoundTopicFilterSuffix;
        int minTopicNameLength;


        public TopicNameRouter()
            : this("mqttTopicRouting")
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="TopicNameRouter" /> class.
        /// </summary>
        /// <param name="configurationSectionName">Name of configuration section that contains routing configuration.</param>
        /// <remarks>
        ///     This constructor uses a section from application configuration to generate routing configuration.
        /// </remarks>
        /// <example>
        ///     <mqttTopicRouting>
        ///         <inboundRoute to="telemetry">
        ///             <template>{deviceId}/messages/events</template>
        ///             <!-- ... -->
        ///         </inboundRoute>
        ///         <outboundRoute from="notification">
        ///             <template>devices/{deviceId}/messages/devicebound/{*subTopic}</template>
        ///         </outboundRoute>
        ///     </mqttTopicRouting>
        /// </example>
        public TopicNameRouter(string configurationSectionName)
        {
            Contract.Requires(!string.IsNullOrEmpty(configurationSectionName));

            var configuration = (RoutingConfiguration)ConfigurationManager.GetSection(configurationSectionName);
            this.InitializeFromConfiguration(configuration);
        }

        public TopicNameRouter(RoutingConfiguration configuration)
        {
            this.InitializeFromConfiguration(configuration);
        }

        void InitializeFromConfiguration(RoutingConfiguration configuration)
        {
            Contract.Requires(configuration != null);

            string serviceBoundTopicFilter = configuration.InboundRoutes.Single().Templates.Single();
            int deviceTemplateParameterIndex = serviceBoundTopicFilter.IndexOf(DeviceTemplateParameter, StringComparison.Ordinal);
            this.serviceBoundTopicFilterPrefix = serviceBoundTopicFilter.Substring(0, deviceTemplateParameterIndex);
            int infixIndex = deviceTemplateParameterIndex + DeviceTemplateParameter.Length;
            int propertiesWildCardIndex = serviceBoundTopicFilter.IndexOf("+", infixIndex, StringComparison.Ordinal);
            this.serviceBoundTopicFilterInfix = serviceBoundTopicFilter.Substring(infixIndex, propertiesWildCardIndex - infixIndex);
            this.serviceBoundTopicFilterSuffix = serviceBoundTopicFilter.Substring(propertiesWildCardIndex + 1);
            this.minTopicNameLength = this.serviceBoundTopicFilterPrefix.Length + 1 + this.serviceBoundTopicFilterInfix.Length;
            this.deviceBoundTopicFilter = configuration.OutboundRoutes.Single().Template;
        }

        /// <summary>
        /// Device-bound mapping
        /// </summary>
        /// <param name="routeType"></param>
        /// <param name="context"></param>
        /// <param name="topicName"></param>
        /// <returns></returns>
        public bool TryMapRouteToTopicName(MqttIotHubAdapter.RouteSourceType routeType, IDictionary<string, string> context, out string topicName)
        {
            string deviceId;
            if (context.TryGetValue(DeviceIdPropertyName, out deviceId))
            {
                string propertiesString = GetPropertiesString(context);
                topicName = this.deviceBoundTopicFilter.Replace(DeviceTemplateParameter, deviceId).Replace(SingleSegmentWildcard, propertiesString);
                return true;
            }
            topicName = null;
            return false;
        }

        /// <summary>
        /// Service-bound mapping
        /// </summary>
        /// <param name="topicName"></param>
        /// <param name="routeType"></param>
        /// <param name="contextOutput"></param>
        /// <returns></returns>
        public bool TryMapTopicNameToRoute(string topicName, out RouteDestinationType routeType, IDictionary<string, string> contextOutput)
        {
            if (string.IsNullOrEmpty(topicName) || topicName.Length < this.minTopicNameLength || !topicName.StartsWith(this.serviceBoundTopicFilterPrefix))
            {
                routeType = RouteDestinationType.Unknown;
                return false;
            }

            string deviceId;
            if (this.TryGetDeviceId(topicName, out deviceId))
            {
                int infixStartIndex = this.serviceBoundTopicFilterPrefix.Length + deviceId.Length;
                if (string.CompareOrdinal(topicName, infixStartIndex, this.serviceBoundTopicFilterInfix, 0, this.serviceBoundTopicFilterInfix.Length) == 0)
                {
                    int propertiesStartIndex = infixStartIndex + this.serviceBoundTopicFilterInfix.Length;
                    ReadProperties(topicName, propertiesStartIndex, contextOutput);

                    this.CheckTopicSuffix(topicName, propertiesStartIndex);
                    routeType = RouteDestinationType.Telemetry;
                    return true;
                }
            }
            routeType = RouteDestinationType.Unknown;
            return false;
        }

        void CheckTopicSuffix(string topicName, int propertiesStartIndex)
        {
            if (topicName.Length > propertiesStartIndex)
            {
                int propertiesEndIndex = topicName.IndexOf(SegmentSeparator, propertiesStartIndex, StringComparison.Ordinal);
                if (propertiesEndIndex > 0)
                {
                    int topicSuffixLength = topicName.EndsWith(SegmentSeparator) ? topicName.Length - propertiesEndIndex - 1 : topicName.Length - propertiesEndIndex;
                    if (string.CompareOrdinal(topicName, propertiesEndIndex, this.serviceBoundTopicFilterSuffix, 0, topicSuffixLength) != 0)
                    {
                        throw new FormatException("Unexpected segment after properties");
                    }
                }
            }
        }

        bool TryGetDeviceId(string topicName, out string deviceId)
        {
            int deviceIdStartIndex = this.serviceBoundTopicFilterPrefix.Length;
            int deviceIdEndIndex = topicName.IndexOf(SegmentSeparator, deviceIdStartIndex, StringComparison.Ordinal);
            if (deviceIdEndIndex > 0)
            {
                deviceId = topicName.Substring(deviceIdStartIndex, deviceIdEndIndex - deviceIdStartIndex);
                return true;
            }
            deviceId = null;
            return false;
        }

        static void ReadProperties(string topicName, int startIndex, IDictionary<string, string> contextOutput)
        {
            if (topicName.Length < startIndex)
            {
                return;
            }
            var parser = new PropertiesParser(contextOutput, topicName, startIndex);
            parser.Parse();
        }

        static string GetPropertiesString(IDictionary<string, string> properties)
        {
            if (properties == null || properties.Count == 0)
            {
                return string.Empty;
            }

            IEnumerator<KeyValuePair<string, string>> propertiesEnumerator = properties.GetEnumerator();

            KeyValuePair<string, string>? firstProperty = null;
            int propertiesCount = 0;
            int estimatedLength = 0;
            while (propertiesEnumerator.MoveNext())
            {
                KeyValuePair<string, string> property = propertiesEnumerator.Current;
                if (PropertiesToIgnore.Contains(property.Key))
                {
                    continue;
                }
                if (propertiesCount == 0)
                {
                    firstProperty = propertiesEnumerator.Current;
                }
                estimatedLength += property.Key.Length + (property.Value != null ? property.Value.Length + 2 : 1);
                propertiesCount++;
            }

            //Optimization for most common case: only correlation ID is present
            if (propertiesCount == 1 && firstProperty.HasValue)
            {
                return firstProperty.Value.Value == null ?
                    Uri.EscapeDataString(firstProperty.Value.Key) :
                    Uri.EscapeDataString(firstProperty.Value.Key) + KeyValueSeparator + Uri.EscapeDataString(firstProperty.Value.Value);
            }

            var propertiesBuilder = new StringBuilder((int)(estimatedLength * EncodedSymbolsFactor));

            propertiesEnumerator.Reset();
            while (propertiesEnumerator.MoveNext())
            {
                KeyValuePair<string, string> property = propertiesEnumerator.Current;
                if (PropertiesToIgnore.Contains(property.Key))
                {
                    continue;
                }
                propertiesBuilder.Append(Uri.EscapeDataString(property.Key));
                if (property.Value != null)
                {
                    propertiesBuilder
                        .Append(KeyValueSeparator)
                        .Append(Uri.EscapeDataString(property.Value));
                }
                propertiesBuilder.Append(PropertySeparator);
            }
            return propertiesBuilder.Length == 0 ? string.Empty : propertiesBuilder.ToString(0, propertiesBuilder.Length - 1);
        }

        class PropertiesParser
        {
            readonly IDictionary<string, string> output;
            readonly Tokenizer tokenizer;

            public PropertiesParser(IDictionary<string, string> output, string value, int startIndex)
            {
                this.output = output;
                this.tokenizer = new Tokenizer(value, startIndex);
            }

            public void Parse()
            {
                string propKey = null;
                string propValue = null;

                foreach (Token token in this.tokenizer.GetTokens())
                {
                    if (propKey == null)
                    {
                        if (token.Type == TokenType.Value)
                        {
                            throw new InvalidOperationException("Unexpected token type: {0}".FormatInvariant(token.Type));
                        }
                        propKey = token.Value;
                        continue;
                    }

                    if (token.Type == TokenType.Key)
                    {
                        this.output[propKey] = propValue;
                        propKey = token.Value;
                        propValue = null;
                    }
                    if (token.Type == TokenType.Value)
                    {
                        propValue = token.Value;
                    }
                }
                if (propKey != null)
                {
                    this.output[propKey] = propValue;
                }
            }

            enum TokenType
            {
                Key,
                Value
            }

            struct Token
            {
                readonly TokenType type;
                readonly string value;

                public TokenType Type
                {
                    get { return this.type; }
                }

                public string Value
                {
                    get { return this.value; }
                }

                public Token(TokenType tokenType, string value)
                {
                    this.type = tokenType;
                    this.value = value == null ? null : Uri.UnescapeDataString(value);
                }
            }

            class Tokenizer
            {
                readonly string value;
                int position;

                TokenType readingTokenType;

                public Tokenizer(string value, int startIndex)
                {
                    this.value = value;
                    this.position = startIndex;
                }

                public IEnumerable<Token> GetTokens()
                {
                    bool endReached = false;
                    while (this.position < this.value.Length && !endReached)
                    {
                        int readCount = 0;
                        int maxCharToReadAllowed = this.value.Length - this.position;
                        switch (this.readingTokenType)
                        {
                            case TokenType.Key:
                                while (true)
                                {
                                    char keyChar = this.value[this.position + readCount];
                                    if (keyChar == SegmentSeparatorChar)
                                    {
                                        if (readCount > 0)
                                        {
                                            yield return new Token(TokenType.Key, this.value.Substring(this.position, readCount));
                                            this.position += readCount;
                                        }
                                        endReached = true;
                                        break;
                                    }
                                    if (readCount == 0 && (keyChar == KeyValueSeparator || keyChar == PropertySeparator))
                                    {
                                        throw new FormatException("Unexpected character {0} at position {1} ".FormatInvariant(keyChar, readCount));
                                    }
                                    if (keyChar == KeyValueSeparator)
                                    {
                                        yield return new Token(TokenType.Key, readCount == 0 ? null : this.value.Substring(this.position, readCount));
                                        this.position += readCount + 1;
                                        this.readingTokenType = TokenType.Value;
                                        break;
                                    }
                                    if (keyChar == PropertySeparator)
                                    {
                                        yield return new Token(TokenType.Key, readCount == 0 ? null : this.value.Substring(this.position, readCount));
                                        this.position += readCount + 1;
                                        this.readingTokenType = TokenType.Key;
                                        break;
                                    }
                                    if (++readCount == maxCharToReadAllowed)
                                    {
                                        if (readCount == 0)
                                        {
                                            throw new FormatException("Unexpected end of string. Key is expected");
                                        }
                                        yield return new Token(TokenType.Key, this.value.Substring(this.position, readCount));
                                        this.position += readCount;
                                        endReached = true;
                                        break;
                                    }
                                }
                                break;
                            case TokenType.Value:
                                while (true)
                                {
                                    char valChar = this.value[this.position + readCount];
                                    if (valChar == SegmentSeparatorChar)
                                    {
                                        if (readCount > 0)
                                        {
                                            yield return new Token(TokenType.Value, this.value.Substring(this.position, readCount));
                                            this.position += readCount;
                                        }
                                        endReached = true;
                                        break;
                                    }
                                    if (valChar == KeyValueSeparator)
                                    {
                                        throw new FormatException("Unexpected character {0} at position {1} ".FormatInvariant(valChar, readCount));
                                    }
                                    if (valChar == PropertySeparator)
                                    {
                                        yield return new Token(TokenType.Value, readCount == 0 ? null : this.value.Substring(this.position, readCount));
                                        this.position += readCount + 1;
                                        if (this.position == this.value.Length)
                                        {
                                            throw new FormatException("Unexpected end of string. Key is expected");
                                        }
                                        this.readingTokenType = TokenType.Key;
                                        break;
                                    }
                                    if (++readCount == maxCharToReadAllowed)
                                    {
                                        yield return new Token(TokenType.Value, readCount == 0 ? null : this.value.Substring(this.position, readCount));
                                        this.position += readCount;
                                        endReached = true;
                                        break;
                                    }
                                }
                                break;
                            default:
                                throw new NotSupportedException("Unknown token type: " + this.readingTokenType);
                        }
                    }
                }
            }
        }
    }
}