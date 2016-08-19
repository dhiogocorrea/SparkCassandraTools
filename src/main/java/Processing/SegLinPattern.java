package Processing;

public class SegLinPattern {

	String rsegda_lin_extrt;
	Integer count;
	Double amount;
	String row_ids;
	String category;
	Double percentual_count;
	Double percentual_amount;
	
	public SegLinPattern(String rsegda_lin_extrt, Integer count, Double amount, String row_ids, String category,
			Double percentual_count, Double percentual_amount) {
		this.rsegda_lin_extrt = rsegda_lin_extrt;
		this.count = count;
		this.amount = amount;
		this.row_ids = row_ids;
		this.category = category;
		this.percentual_count = percentual_count;
		this.percentual_amount = percentual_amount;
	}

	public String getRsegda_lin_extrt() {
		return rsegda_lin_extrt;
	}

	public void setRsegda_lin_extrt(String rsegda_lin_extrt) {
		this.rsegda_lin_extrt = rsegda_lin_extrt;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public Double getAmount() {
		return amount;
	}

	public void setAmount(Double amount) {
		this.amount = amount;
	}

	public String getRow_ids() {
		return row_ids;
	}

	public void setRow_ids(String row_ids) {
		this.row_ids = row_ids;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public Double getPercentual_count() {
		return percentual_count;
	}

	public void setPercentual_count(Double percentual_count) {
		this.percentual_count = percentual_count;
	}

	public Double getPercentual_amount() {
		return percentual_amount;
	}

	public void setPercentual_amount(Double percentual_amount) {
		this.percentual_amount = percentual_amount;
	}
	
	
}
