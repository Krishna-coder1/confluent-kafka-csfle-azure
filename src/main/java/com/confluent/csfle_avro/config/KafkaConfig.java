package com.confluent.csfle_avro.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import com.confluent.dto.PersonalData;

@Configuration
public class KafkaConfig {

        // Common Kafka Properties
        @Value("${kafka.bootstrap.servers}")
        private String bootstrapServers;
        @Value("${kafka.security.protocol}")
        private String securityProtocol;
        @Value("${kafka.sasl.mechanism}")
        private String saslMechanism;
        @Value("${kafka.sasl.jaas.config}")
        private String saslJaasConfig;

        // Schema Registry Properties
        @Value("${schema.registry.url}")
        private String schemaRegistryUrl;
        @Value("${schema.registry.basic.auth.credentials.source}")
        private String schemaRegistryAuthSource;
        @Value("${schema.registry.basic.auth.user.info}")
        private String schemaRegistryAuthUserInfo;
        @Value("${schema.auto.register.schemas}")
        private String autoRegisterSchemas;
        @Value("${schema.use.latest.version}")
        private String useLatestVersion;

        // CSFLE Properties
        @Value("${csfle.rule.executors.default.param.tenant.id}")
        private String csfleTenantId;
        @Value("${csfle.rule.executors.default.param.client.id}")
        private String csfleClientId;
        @Value("${csfle.rule.executors.default.param.client.secret}")
        private String csfleClientSecret;

        // Producer Specific Properties
        @Value("${kafka.producer.key.serializer}")
        private String producerKeySerializer;
        @Value("${kafka.producer.value.serializer}")
        private String producerValueSerializer;
        @Value("${kafka.producer.session.timeout.ms}")
        private String producerSessionTimeoutMs;
        @Value("${kafka.producer.request.timeout.ms}")
        private String producerRequestTimeoutMs;
        @Value("${kafka.producer.delivery.timeout.ms}")
        private String producerDeliveryTimeoutMs;

        // Consumer Specific Properties
        @Value("${kafka.consumer.key.deserializer}")
        private String consumerKeyDeserializer;
        @Value("${kafka.consumer.value.deserializer}")
        private String consumerValueDeserializer;
        @Value("${kafka.consumer.fetch.min.bytes}")
        private String consumerFetchMinBytes;
        @Value("${kafka.consumer.fetch.max.bytes}")
        private String consumerFetchMaxBytes;
        @Value("${kafka.consumer.fetch.max.wait.ms}")
        private String consumerFetchMaxWaitMs;
        @Value("${kafka.consumer.max.poll.records}")
        private String consumerMaxPollRecords;
        @Value("${kafka.consumer.max.poll.interval.ms}")
        private String consumerMaxPollIntervalMs;
        @Value("${kafka.consumer.session.timeout.ms}")
        private String consumerSessionTimeoutMs;
        @Value("${kafka.consumer.heartbeat.interval.ms}")
        private String consumerHeartbeatIntervalMs;
        @Value("${kafka.consumer.request.timeout.ms}")
        private String consumerRequestTimeoutMs;
        @Value("${kafka.consumer.client.id}")
        private String consumerClientId;

        // Kafka Listener Specific Properties
        @Value("${kafka.listener.concurrency}")
        private Integer listenerConcurrency;

        @Bean
        public ProducerFactory<String, PersonalData> producerFactory() {
                Map<String, Object> configProps = new HashMap<>();

                // Core Kafka
                configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
                configProps.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
                configProps.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

                // Serializers
                configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, producerKeySerializer);
                configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, producerValueSerializer);

                // Timeouts
                configProps.put("session.timeout.ms", producerSessionTimeoutMs);
                configProps.put("request.timeout.ms", producerRequestTimeoutMs);
                configProps.put("delivery.timeout.ms", producerDeliveryTimeoutMs);

                // Schema Registry
                configProps.put("schema.registry.url", schemaRegistryUrl);
                configProps.put("basic.auth.credentials.source", schemaRegistryAuthSource);
                configProps.put("schema.registry.basic.auth.user.info", schemaRegistryAuthUserInfo);

                // Schema handling
                configProps.put("auto.register.schemas", autoRegisterSchemas);
                configProps.put("use.latest.version", useLatestVersion);

                // CSFLE (Custom encryption props)
                configProps.put("rule.executors._default_.param.tenant.id", csfleTenantId);
                configProps.put("rule.executors._default_.param.client.id", csfleClientId);
                configProps.put("rule.executors._default_.param.client.secret", csfleClientSecret);

                return new DefaultKafkaProducerFactory<>(configProps);
        }

        @Bean
        public KafkaTemplate<String, PersonalData> kafkaTemplate() {
                return new KafkaTemplate<>(producerFactory());
        }

        @Bean
        public ConsumerFactory<String, PersonalData> consumerFactory() {
                Map<String, Object> configProps = new HashMap<>();

                // Core Kafka
                configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
                configProps.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
                configProps.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

                // Deserializers
                configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, consumerKeyDeserializer);
                configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, consumerValueDeserializer);

                // Schema Registry
                configProps.put("schema.registry.url", schemaRegistryUrl);
                configProps.put("basic.auth.credentials.source", schemaRegistryAuthSource);
                configProps.put("schema.registry.basic.auth.user.info", schemaRegistryAuthUserInfo);

                // Schema handling
                configProps.put("auto.register.schemas", autoRegisterSchemas);
                configProps.put("use.latest.version", useLatestVersion);

                // CSFLE (Custom encryption props)
                configProps.put("rule.executors._default_.param.tenant.id", csfleTenantId);
                configProps.put("rule.executors._default_.param.client.id", csfleClientId);
                configProps.put("rule.executors._default_.param.client.secret", csfleClientSecret);

                // Performance tuning
                configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, consumerFetchMinBytes);
                configProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, consumerFetchMaxBytes);
                configProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, consumerFetchMaxWaitMs);
                configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerMaxPollRecords);
                configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, consumerMaxPollIntervalMs);
                configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, consumerSessionTimeoutMs);
                configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, consumerHeartbeatIntervalMs);
                configProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, consumerRequestTimeoutMs);
                configProps.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId);

                return new DefaultKafkaConsumerFactory<>(configProps);
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, PersonalData> kafkaListenerContainerFactory() {
                ConcurrentKafkaListenerContainerFactory<String, PersonalData> factory = new ConcurrentKafkaListenerContainerFactory<>();
                factory.setConsumerFactory(consumerFactory());
                factory.setConcurrency(listenerConcurrency);
                return factory;
        }
}