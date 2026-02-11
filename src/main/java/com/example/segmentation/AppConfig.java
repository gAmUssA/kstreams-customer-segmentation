package com.example.segmentation;

import io.github.cdimascio.dotenv.Dotenv;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.streams.StreamsConfig;

public class AppConfig {

    private final String mode;
    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String applicationId;
    private final String inputTopic;
    private final String outputTopic;
    private final String apiKey;
    private final String apiSecret;
    private final String srApiKey;
    private final String srApiSecret;
    private final int topicPartitions;
    private final short topicReplicationFactor;

    public AppConfig(Dotenv dotenv) {
        this.mode = dotenv.get("MODE", "local");
        this.bootstrapServers = dotenv.get(
            "CC_BOOTSTRAP_SERVER",
            "localhost:9092"
        );
        this.schemaRegistryUrl = dotenv.get(
            "CC_SCHEMA_REGISTRY_URL",
            "http://localhost:8081"
        );
        this.applicationId = dotenv.get(
            "APPLICATION_ID",
            "customer-segmentation-app"
        );
        this.inputTopic = dotenv.get("INPUT_TOPIC", "orders");
        this.outputTopic = dotenv.get("OUTPUT_TOPIC", "customer-segments");
        this.apiKey = dotenv.get("CC_API_KEY", "");
        this.apiSecret = dotenv.get("CC_API_SECRET", "");
        this.srApiKey = dotenv.get("CC_SR_API_KEY", "");
        this.srApiSecret = dotenv.get("CC_SR_API_SECRET", "");
        this.topicPartitions = Integer.parseInt(
            dotenv.get("TOPIC_PARTITIONS", "1")
        );
        this.topicReplicationFactor = Short.parseShort(
            dotenv.get("TOPIC_REPLICATION_FACTOR", "1")
        );
    }

    public boolean isCloudMode() {
        return "cloud".equals(mode);
    }

    public String getMode() {
        return mode;
    }

    public String getInputTopic() {
        return inputTopic;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public int getTopicPartitions() {
        return topicPartitions;
    }

    public short getTopicReplicationFactor() {
        return topicReplicationFactor;
    }

    public Properties streamsProperties() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if (isCloudMode()) {
            addSaslProperties(props);
        }
        return props;
    }

    public Properties adminClientProperties() {
        var props = new Properties();
        props.put(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServers
        );
        if (isCloudMode()) {
            addSaslProperties(props);
        }
        return props;
    }

    public Map<String, String> schemaRegistryConfig() {
        var config = new HashMap<String, String>();
        config.put("schema.registry.url", schemaRegistryUrl);
        if (isCloudMode()) {
            config.put("basic.auth.credentials.source", "USER_INFO");
            config.put("basic.auth.user.info", srApiKey + ":" + srApiSecret);
        }
        return config;
    }

    private void addSaslProperties(Properties props) {
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(
            SaslConfigs.SASL_JAAS_CONFIG,
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"" +
                apiKey +
                "\" " +
                "password=\"" +
                apiSecret +
                "\";"
        );
    }
}
