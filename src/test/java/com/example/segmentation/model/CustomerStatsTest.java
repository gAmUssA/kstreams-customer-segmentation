package com.example.segmentation.model;

import com.example.segmentation.serde.JsonSerde;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CustomerStatsTest {

    @Test
    void newCustomerWithSingleSmallOrder() {
        CustomerStats stats = new CustomerStats();
        stats.add(new Order("o1", "c1", 50.0, 1000L, "electronics"));

        assertThat(stats.computeSegment()).isEqualTo("New");
        assertThat(stats.getOrderCount()).isEqualTo(1);
        assertThat(stats.getTotalSpent()).isEqualTo(50.0);
    }

    @Test
    void regularCustomerByOrderCount() {
        CustomerStats stats = new CustomerStats();
        stats.add(new Order("o1", "c1", 10.0, 1000L, "books"));
        stats.add(new Order("o2", "c1", 10.0, 2000L, "books"));

        assertThat(stats.computeSegment()).isEqualTo("Regular");
    }

    @Test
    void regularCustomerByTotalSpent() {
        CustomerStats stats = new CustomerStats();
        stats.add(new Order("o1", "c1", 100.0, 1000L, "electronics"));

        assertThat(stats.computeSegment()).isEqualTo("Regular");
    }

    @Test
    void premiumCustomerByTotalSpent() {
        CustomerStats stats = new CustomerStats();
        stats.add(new Order("o1", "c1", 500.0, 1000L, "jewelry"));

        assertThat(stats.computeSegment()).isEqualTo("Premium");
    }

    @Test
    void premiumCustomerByOrderCount() {
        CustomerStats stats = new CustomerStats();
        for (int i = 0; i < 5; i++) {
            stats.add(new Order("o" + i, "c1", 10.0, 1000L * i, "books"));
        }

        assertThat(stats.computeSegment()).isEqualTo("Premium");
    }

    @Test
    void vipCustomerRequiresBothConditions() {
        CustomerStats stats = new CustomerStats();
        for (int i = 0; i < 10; i++) {
            stats.add(new Order("o" + i, "c1", 100.0, 1000L * i, "electronics"));
        }

        assertThat(stats.getTotalSpent()).isEqualTo(1000.0);
        assertThat(stats.getOrderCount()).isEqualTo(10);
        assertThat(stats.computeSegment()).isEqualTo("VIP");
    }

    @Test
    void highSpendLowCountIsPremiumNotVip() {
        CustomerStats stats = new CustomerStats();
        stats.add(new Order("o1", "c1", 1500.0, 1000L, "jewelry"));

        assertThat(stats.getTotalSpent()).isEqualTo(1500.0);
        assertThat(stats.getOrderCount()).isEqualTo(1);
        assertThat(stats.computeSegment()).isEqualTo("Premium");
    }

    @Test
    void singleCategoryBehavior() {
        CustomerStats stats = new CustomerStats();
        stats.add(new Order("o1", "c1", 10.0, 1000L, "books"));
        stats.add(new Order("o2", "c1", 20.0, 2000L, "books"));

        assertThat(stats.computePurchaseBehavior()).isEqualTo("Single-Category");
    }

    @Test
    void multiCategoryBehavior() {
        CustomerStats stats = new CustomerStats();
        stats.add(new Order("o1", "c1", 10.0, 1000L, "books"));
        stats.add(new Order("o2", "c1", 20.0, 2000L, "electronics"));

        assertThat(stats.computePurchaseBehavior()).isEqualTo("Multi-Category");
    }

    @Test
    void diverseBehavior() {
        CustomerStats stats = new CustomerStats();
        stats.add(new Order("o1", "c1", 10.0, 1000L, "books"));
        stats.add(new Order("o2", "c1", 20.0, 2000L, "electronics"));
        stats.add(new Order("o3", "c1", 30.0, 3000L, "clothing"));
        stats.add(new Order("o4", "c1", 40.0, 4000L, "food"));

        assertThat(stats.computePurchaseBehavior()).isEqualTo("Diverse");
    }

    @Test
    void avgOrderValueCalculation() {
        CustomerStats stats = new CustomerStats();
        stats.add(new Order("o1", "c1", 100.0, 1000L, "a"));
        stats.add(new Order("o2", "c1", 200.0, 2000L, "b"));

        assertThat(stats.getAvgOrderValue()).isEqualTo(150.0);
    }

    @Test
    void duplicateCategoriesNotDoubleCounted() {
        CustomerStats stats = new CustomerStats();
        stats.add(new Order("o1", "c1", 10.0, 1000L, "books"));
        stats.add(new Order("o2", "c1", 20.0, 2000L, "books"));
        stats.add(new Order("o3", "c1", 30.0, 3000L, "books"));

        assertThat(stats.getCategoriesCount()).isEqualTo(1);
        assertThat(stats.computePurchaseBehavior()).isEqualTo("Single-Category");
    }

    @Test
    void serdeRoundTrip() {
        JsonSerde<CustomerStats> serde = new JsonSerde<>(CustomerStats.class);
        CustomerStats original = new CustomerStats();
        original.add(new Order("o1", "c1", 100.0, 1000L, "books"));
        original.add(new Order("o2", "c1", 200.0, 2000L, "electronics"));

        byte[] bytes = serde.serializer().serialize("test-topic", original);
        CustomerStats deserialized = serde.deserializer().deserialize("test-topic", bytes);

        assertThat(deserialized.getTotalSpent()).isEqualTo(300.0);
        assertThat(deserialized.getOrderCount()).isEqualTo(2);
        assertThat(deserialized.getCategoriesCount()).isEqualTo(2);
        assertThat(deserialized.computeSegment()).isEqualTo(original.computeSegment());
        assertThat(deserialized.computePurchaseBehavior()).isEqualTo(original.computePurchaseBehavior());
    }
}
