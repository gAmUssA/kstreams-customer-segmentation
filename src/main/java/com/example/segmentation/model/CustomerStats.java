package com.example.segmentation.model;

import com.example.segmentation.avro.Order;
import java.util.HashSet;
import java.util.Set;

public class CustomerStats {

    private double totalSpent;
    private int orderCount;
    private Set<String> categoriesPurchased;

    public CustomerStats() {
        this.totalSpent = 0.0;
        this.orderCount = 0;
        this.categoriesPurchased = new HashSet<>();
    }

    public CustomerStats add(Order order) {
        this.totalSpent += order.getAmount();
        this.orderCount++;
        this.categoriesPurchased.add(order.getCategory());
        return this;
    }

    public double getAvgOrderValue() {
        return orderCount == 0 ? 0.0 : totalSpent / orderCount;
    }

    public int getCategoriesCount() {
        return categoriesPurchased.size();
    }

    public String computeSegment() {
        if (totalSpent >= 1000 && orderCount >= 10) return "VIP";
        if (totalSpent >= 500 || orderCount >= 5) return "Premium";
        if (totalSpent >= 100 || orderCount >= 2) return "Regular";
        return "New";
    }

    public String computePurchaseBehavior() {
        int count = getCategoriesCount();
        if (count >= 4) return "Diverse";
        if (count >= 2) return "Multi-Category";
        return "Single-Category";
    }

    public double getTotalSpent() {
        return totalSpent;
    }

    public void setTotalSpent(double totalSpent) {
        this.totalSpent = totalSpent;
    }

    public int getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(int orderCount) {
        this.orderCount = orderCount;
    }

    public Set<String> getCategoriesPurchased() {
        return categoriesPurchased;
    }

    public void setCategoriesPurchased(Set<String> categoriesPurchased) {
        this.categoriesPurchased = categoriesPurchased;
    }
}
