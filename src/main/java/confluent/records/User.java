package confluent.records;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Objects;

@JsonPropertyOrder({"customer_id", "name", "mail"})
public final class User {

    public String customer_id;
    public String name;
    public String mail;

    public User() {
    }

    public User(String customer_id, String name, String mail) {
        this.customer_id = customer_id;
        this.name = name;
        this.mail = mail;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return Objects.equals(customer_id, user.customer_id) &&
                Objects.equals(name, user.name) &&
                Objects.equals(mail, user.mail);
    }

    @Override
    public int hashCode() {
        return Objects.hash(customer_id, name, mail);
    }

    @Override
    public String toString() {
        return "User{" +
                "userid='" + customer_id + '\'' +
                ", name='" + name + '\'' +
                ", mail='" + mail + '\'' +
                '}';
    }

}
