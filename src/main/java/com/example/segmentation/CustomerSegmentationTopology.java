package com.example.segmentation;

import com.example.segmentation.avro.CustomerSegment;
import com.example.segmentation.avro.Order;
import com.example.segmentation.model.CustomerStats;
import com.example.segmentation.serde.JsonSerde;
import com.example.segmentation.util.CustomerSegmentMapper;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomerSegmentationTopology {

    private static final Logger log = LoggerFactory.getLogger(
        CustomerSegmentationTopology.class
    );

    public static final String INPUT_TOPIC = "orders";
    public static final String OUTPUT_TOPIC = "customer-segments";
    public static final String STATS_STORE = "customer-stats-store";

    public static Topology build(Map<String, String> schemaRegistryConfig) {
        log.info(
            "Building customer segmentation topology: {} -> {}",
            INPUT_TOPIC,
            OUTPUT_TOPIC
        );
        var builder = new StreamsBuilder();

        var stringSerde = Serdes.String();
        var statsSerde = new JsonSerde<>(CustomerStats.class);

        var orderSerde = new SpecificAvroSerde<Order>();
        orderSerde.configure(schemaRegistryConfig, false);

        var segmentSerde = new SpecificAvroSerde<CustomerSegment>();
        segmentSerde.configure(schemaRegistryConfig, false);

        var orders = builder.stream(
            INPUT_TOPIC,
            Consumed.with(stringSerde, orderSerde)
        );

        var statsTable = orders
            .selectKey((key, order) -> order.getCustomerId())
            .groupByKey(Grouped.with(stringSerde, orderSerde))
            .aggregate(
                CustomerStats::new,
                (customerId, order, stats) -> stats.add(order),
                Materialized.<
                        String,
                        CustomerStats,
                        KeyValueStore<Bytes, byte[]>
                    >as(STATS_STORE)
                    .withKeySerde(stringSerde)
                    .withValueSerde(statsSerde)
            );

        statsTable
            .mapValues(
                CustomerSegmentMapper::fromStats,
                Materialized.with(stringSerde, segmentSerde)
            )
            .toStream()
            .to(OUTPUT_TOPIC, Produced.with(stringSerde, segmentSerde));

        var topology = builder.build();
        log.debug("Topology description:\n{}", topology.describe());
        return topology;
    }
}
