package com.example.segmentation.model;

public class CustomerSegment {

    private String customerId;
    private double totalSpent;
    private int orderCount;
    private double avgOrderValue;
    private int categoriesPurchased;
    private String customerSegment;
    private String purchaseBehavior;

    public CustomerSegment() {
    }

    public static CustomerSegment fromStats(String customerId, CustomerStats stats) {
        CustomerSegment segment = new CustomerSegment();
        segment.customerId = customerId;
        segment.totalSpent = stats.getTotalSpent();
        segment.orderCount = stats.getOrderCount();
        segment.avgOrderValue = stats.getAvgOrderValue();
        segment.categoriesPurchased = stats.getCategoriesCount();
        segment.customerSegment = stats.computeSegment();
        segment.purchaseBehavior = stats.computePurchaseBehavior();
        return segment;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
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

    public double getAvgOrderValue() {
        return avgOrderValue;
    }

    public void setAvgOrderValue(double avgOrderValue) {
        this.avgOrderValue = avgOrderValue;
    }

    public int getCategoriesPurchased() {
        return categoriesPurchased;
    }

    public void setCategoriesPurchased(int categoriesPurchased) {
        this.categoriesPurchased = categoriesPurchased;
    }

    public String getCustomerSegment() {
        return customerSegment;
    }

    public void setCustomerSegment(String customerSegment) {
        this.customerSegment = customerSegment;
    }

    public String getPurchaseBehavior() {
        return purchaseBehavior;
    }

    public void setPurchaseBehavior(String purchaseBehavior) {
        this.purchaseBehavior = purchaseBehavior;
    }
}
