package Processing;

import java.io.Serializable;


public class CountPattern implements Serializable {
    
    String text;
    Integer count;
    Double value;
    String category;
    String prefix;
    String financial_institution;
    String clients;
    Integer count_clients;
    
     
    
	public CountPattern(String text, Integer count, Double value, String category, String prefix,
			String financial_institution, String clients, Integer count_clients) {
		this.text = text;
		this.count = count;
		this.value = value;
		this.category = category;
		this.prefix = prefix;
		this.financial_institution = financial_institution;
		this.clients = clients;
		this.count_clients = count_clients;
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
	public String getPrefix() {
		return prefix;
	}
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	public String getFinancial_institution() {
		return financial_institution;
	}
	public void setFinancial_institution(String financial_institution) {
		this.financial_institution = financial_institution;
	}
	public String getClients() {
		return clients;
	}
	public void setClients(String clients) {
		this.clients = clients;
	}
	public Integer getCount_clients() {
		return count_clients;
	}
	public void setCount_clients(Integer count_clients) {
		this.count_clients = count_clients;
	}
    
}
