package com.example.segmentation;

import com.example.segmentation.model.CustomerSegment;
import com.example.segmentation.model.CustomerStats;
import com.example.segmentation.model.Order;
import com.example.segmentation.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class CustomerSegmentationTopology {

    public static final String INPUT_TOPIC = "orders";
    public static final String OUTPUT_TOPIC = "customer-segments";
    public static final String STATS_STORE = "customer-stats-store";

    public static Topology build() {
        var builder = new StreamsBuilder();

        var stringSerde = Serdes.String();
        var orderSerde = new JsonSerde<>(Order.class);
        var statsSerde = new JsonSerde<>(CustomerStats.class);
        var segmentSerde = new JsonSerde<>(CustomerSegment.class);

        var orders = builder.stream(
                INPUT_TOPIC,
                Consumed.with(stringSerde, orderSerde));

        var statsTable = orders
                .selectKey((key, order) -> order.getCustomerId())
                .groupByKey(Grouped.with(stringSerde, orderSerde))
                .aggregate(
                        CustomerStats::new,
                        (customerId, order, stats) -> stats.add(order),
                        Materialized.<String, CustomerStats, KeyValueStore<Bytes, byte[]>>as(STATS_STORE)
                                .withKeySerde(stringSerde)
                                .withValueSerde(statsSerde));

        statsTable
                .mapValues((customerId, stats) -> CustomerSegment.fromStats(customerId, stats),
                        Materialized.with(stringSerde, segmentSerde))
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(stringSerde, segmentSerde));

        return builder.build();
    }
}
