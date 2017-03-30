package sampler;

public class ExitCodeException extends Exception {
	private static final long serialVersionUID = 1L;
	private int exitCode = 1;
	
	public ExitCodeException(int exitCode){
		this.exitCode = exitCode;
	}
	
	public int getExitCode(){
		return this.exitCode;
	}
	
}
