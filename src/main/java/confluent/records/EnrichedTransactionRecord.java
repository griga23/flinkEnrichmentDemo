package confluent.records;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;

public class EnrichedTransactionRecord {

    @JsonFormat(
            shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            timezone = "UTC")
    public Instant ts;
    public String id;
    public String userId;
    public String user;
    public String email;
    public String productId;
    public String product;
    public String category;
    public Integer amount;
    public float unitPrice;
    public float totalPrice;

    public EnrichedTransactionRecord() {
    }

    public EnrichedTransactionRecord(Instant ts, String id, String userId, String user, String email, String productId, String product, String category, Integer amount, float unitPrice, float totalPrice) {
        this.ts = ts;
        this.id = id;
        this.userId = userId;
        this.user = user;
        this.email = email;
        this.productId = productId;
        this.product = product;
        this.category = category;
        this.amount = amount;
        this.unitPrice = unitPrice;
        this.totalPrice = totalPrice;
    }

    @Override
    public String toString() {
        return "EnrichedTransactionRecord{" +
                "ts=" + ts +
                ", id='" + id + '\'' +
                ", userId='" + userId + '\'' +
                ", user='" + user + '\'' +
                ", email='" + email + '\'' +
                ", productId='" + productId + '\'' +
                ", product='" + product + '\'' +
                ", category='" + category + '\'' +
                ", amount=" + amount +
                ", unitPrice=" + unitPrice +
                ", totalPrice=" + totalPrice +
                '}';
    }
}
