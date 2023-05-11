package confluent.records;

import java.util.Objects;

public class LoyaltyLevel {
    public String user_id;
    public float sum;
    public String level;

    public LoyaltyLevel() {
    }

    public LoyaltyLevel(String user_id, float sum, String level) {
        this.user_id = user_id;
        this.sum = sum;
        this.level = level;
    }

    @Override
    public String toString() {
        return "LoyaltyLevel{" +
                "user_id='" + user_id + '\'' +
                ", sum=" + sum +
                ", level='" + level + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoyaltyLevel that = (LoyaltyLevel) o;
        return Float.compare(that.sum, sum) == 0 &&
                Objects.equals(user_id, that.user_id) &&
                Objects.equals(level, that.level);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user_id, sum, level);
    }
}
