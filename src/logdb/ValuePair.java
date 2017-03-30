package logdb;

public class ValuePair {
	private String name;
	private String value;
	
	public ValuePair(String name, String value){
		this.name = name;
		this.value = value;
	}
	
	public String getName(){
		return name;
	}
	
	public String getValue(){
		return value;
	}
	
}
