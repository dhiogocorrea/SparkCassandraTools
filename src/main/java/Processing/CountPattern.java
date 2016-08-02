package Processing;

import java.io.Serializable;


public class CountPattern implements Serializable {
    
    String text;
    Integer count;
    Double value;
    String category;
    
    public CountPattern(String text, Integer count, Double value, String category) {
        this.text = text;
        this.count = count;
        this.value = value;
        this.category = category;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }
    
    
}
