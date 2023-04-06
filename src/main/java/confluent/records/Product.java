package confluent.records;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Objects;

@JsonPropertyOrder({"prod_id", "product", "category","price"})

public class Product {

    public String prod_id;
    public String product;
    public String category;
    public Float price;


    public Product() {
    }

    public Product(String prod_id, String product, String category, Float price) {
        this.prod_id = prod_id;
        this.product = product;
        this.category = category;
        this.price = price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Product product1 = (Product) o;
        return Objects.equals(prod_id, product1.prod_id) &&
                Objects.equals(product, product1.product) &&
                Objects.equals(category, product1.category) &&
                Objects.equals(price, product1.price);
    }

    @Override
    public int hashCode() {
        return Objects.hash(prod_id, product, category, price);
    }

    @Override
    public String toString() {
        return "Product{" +
                "prod_id='" + prod_id + '\'' +
                ", product='" + product + '\'' +
                ", category='" + category + '\'' +
                ", price=" + price +
                '}';
    }
}
