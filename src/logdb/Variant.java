package logdb;

public class Variant {
	private int pos;
	private String chrom;
	private String content;
	
	public Variant(String chrom, int pos, String content){
		this.setChrom(chrom);
		this.setPos(pos);
		this.setContent(content);
	}

	public String getChrom() {
		return chrom;
	}

	private void setChrom(String chrom) {
		this.chrom = chrom;
	}

	public int getPos() {
		return pos;
	}

	private void setPos(int pos) {
		this.pos = pos;
	}

	public String getContent() {
		return content;
	}

	private void setContent(String content) {
		this.content = content;
	}
	
}
