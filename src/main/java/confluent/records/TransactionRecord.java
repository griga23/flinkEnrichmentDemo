package confluent.records;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;
import java.util.Objects;

public class TransactionRecord {

    @JsonFormat(
            shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            timezone = "UTC")
    public Instant time;
    public String id;
    public String user_id;
    public String product_id;
    public Integer amount;


    public TransactionRecord() {
    }

    public TransactionRecord(Instant time, String id, String user_id, String product_id, Integer amount) {
        this.time = time;
        this.id = id;
        this.user_id = user_id;
        this.product_id = product_id;
        this.amount = amount;
    }


    @Override
    public String toString() {
        return "TransactionRecord{"+
                ", user_id='" + user_id + '\'' +
                ", product_id='" + product_id + '\'' +
                ", amount=" + amount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionRecord that = (TransactionRecord) o;
        return Objects.equals(time, that.time) &&
                Objects.equals(id, that.id) &&
                Objects.equals(user_id, that.user_id) &&
                Objects.equals(product_id, that.product_id) &&
                Objects.equals(amount, that.amount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(time, id, user_id, product_id, amount);
    }
}
