package general;

public class ExitCode {
	public static final int NOERROR = 0;
	public static final int MISUSEERROR = 2;
	public static final int UNKNOWNERROR = 1;
	public static final int EXECUTEERROR = 3;
	public static final int INPUTERROR = 4;
	public static final int PATHERROR = 5;
	public static final int ARGUMENTERROR = 6;
	public static final int WORKFLOWERROR = 7;
	public static final int INVOKEERROR = 126;
	public static final int COMMANDNOTFOUNDERROR = 127;
	
	public static String getExitCode(int code){
		switch(code){
		case NOERROR: return "No error";
		case MISUSEERROR: return "Misuse of shell builtins (according to Bash documentation)";
		case PATHERROR: return "Path error";
		case EXECUTEERROR: return "Execute error";
		case INPUTERROR: return "Input error";
		case ARGUMENTERROR: return "Argument error";
		case WORKFLOWERROR: return "Error in the workflow definition";
		case INVOKEERROR: return "Command invoked cannot execute";
		case COMMANDNOTFOUNDERROR: return "Command not found";
		
		default: return "Unknown error: "+code;
		}
	}
}
