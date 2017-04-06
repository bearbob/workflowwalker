package general;

import instances.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.*;
import java.util.logging.FileHandler;

import logdb.LogDB;
import sampler.Walker;

public class Main {
	private static Logger logger;
	private static final String VERSION = "1.46b";
	//defaults
	private static String runName = "dna";
	private static String inputFile = "";
	private static String baseDir = "~/workspace";
	private static String dbname = "log.db"; //default name
	private static int sampleNumber = 100;
	private static int threadNumber = 1;
	private static int randomSeed = 42;
	
	
	public static void main(String[] args) {
		run(args);
	}
	
	private static void run(String[] args) {
		
		ArrayList<String> nargs = new ArrayList<>(Arrays.asList(args));
		if(nargs.contains("-h") || nargs.contains("--help")){
			showHelp("");
		}
		
		//initiate the logger after the help window to prevent logs where only the help menu was called
		startLogger();

		StringBuilder builder = new StringBuilder();
		for(String s : args) {
		    builder.append(s);
		    builder.append(" ");
		}
		logger.info("Given command string is: '"+builder.toString().trim()+"'");

		//check for arguments
		if(nargs.contains("-r") || nargs.contains("--run")){
			int pos = Math.max(nargs.indexOf("-r"), nargs.indexOf("--run"));
			try{
				String val = nargs.get(pos+1);
				if(val.startsWith("-")){
					showHelp("Value expected after run option but next option found: "+val);
				}
				runName = val;
			}catch(IndexOutOfBoundsException ie){
				showHelp("Argument expected after run option but none found.");
			}
		}
		logger.finest("Run name handled.");

		String[][] parameter = {
				new String[]{"Seed", "seed", "seed"},
				new String[]{"Threads", "t", "threads"},
				new String[]{"Sample", "s", "sample"}
		};
		int[] pint = {randomSeed, threadNumber, sampleNumber};
		for(int i=0; i<parameter.length; i++) {
			String pattern1 = "-"+parameter[i][1];
			String pattern2 = "--"+parameter[i][2];
			if (nargs.contains(pattern1) || nargs.contains(pattern2)) {
				int pos = Math.max(nargs.indexOf(pattern1), nargs.indexOf(pattern2));
				logger.finest("Position for "+parameter[i][0]+" " + pos);
				try {
					String val = nargs.get(pos + 1);
					if (val.startsWith("-")) {
						showHelp("Value expected after "+parameter[i][0]+" option but next option found instead: " + val);
					}
					pint[i] = Integer.parseInt(val);
					logger.finest(parameter[i][0]+" value: '" + randomSeed + "'");
				} catch (IndexOutOfBoundsException ie) {
					showHelp("Argument expected after seed option but none found.");
				}
			}
			logger.finest(parameter[i][0]+" handled.");
		}
		randomSeed = pint[0];
		threadNumber = pint[1];
		sampleNumber = pint[2];

		if(nargs.contains("-d") || nargs.contains("--database")){
			int pos = Math.max(nargs.indexOf("-d"), nargs.indexOf("--database"));
			logger.finest("Position for -d or --database: "+pos);
			try{
				String val = nargs.get(pos+1);
				if(val.startsWith("-")){
					showHelp("Value expected after database option but next option found: "+val);
				}
				logger.finest("Database name: '"+val+"'");
				dbname = val;
			}catch(IndexOutOfBoundsException ie){
				showHelp("Argument expected after database option but none found.");
			}
		}
		logger.finest("Database name handled.");

		LogDB logdb = new LogDB(dbname, randomSeed);
		TargetFunction tf = new TargetFunction(logdb);

		//this is for testing the walker with a simple command
		if(nargs.contains("--test")){
			// RastriginWalker is a basic function to test the functionality
			Walker test = new RastriginWalker(logdb, runName);
			test.sample(sampleNumber);
			System.exit(0);
		}

		if(nargs.contains("-i") || nargs.contains("--input")){
			int pos = Math.max(nargs.indexOf("-i"), nargs.indexOf("--input"));
			try{
				String val = nargs.get(pos+1);
				if(val.startsWith("-")){
					showHelp("Value expected after input option but next option found: "+val);
				}
				inputFile = val;
			}catch(IndexOutOfBoundsException ie){
				showHelp("Argument expected after input option but none found.");
			}
		}else{
			showHelp("Missing input file as argument.");
		}
		logger.finest("Input file handled.");
		
		if(nargs.contains("-b") || nargs.contains("--base")){
			int pos = Math.max(nargs.indexOf("-b"), nargs.indexOf("--base"));
			try{
				String val = nargs.get(pos+1);
				if(val.startsWith("-")){
					showHelp("Value expected after base option but next option found: "+val);
				}
				baseDir = val;
			}catch(IndexOutOfBoundsException ie){
				showHelp("Argument expected after base option but none found.");
			}
		}
		logger.finest("Base path handled.");
		
		boolean useCache = true;
		if(nargs.contains("--no-cache")){
			logger.info("Cache will be deactivated.");
			useCache = false;
		}
		
		if(nargs.contains("--gold")){
			logger.info("Changed target function to comparison with gold standard");
			tf.setTarget(TargetFunction.SIMILARITYgoldstandard);
		}else{
			logger.info("Target function set to meta comparison with a burry in of 20 samples.");
		}

		if(nargs.contains("--overwrite")){
			logger.info("Target function will overwrite old gold standards if any exist.");
			tf.setOverwrite(true);
		}
		
		if(nargs.contains("--anno")){
			logger.info("Workflow will be extended: annotating will be performed.");
			tf.setAnnotate(true);
		}

		logger.info("Run values:\n\tbaseDir: "+baseDir+"\n\trunName: "+runName+"\n\tsampleNumber: "+sampleNumber);
		
		if(nargs.contains("--rna")){
			if(nargs.contains("--ploidy")){
				Walker ploid = new PloidyRnaWalker(logdb, runName, baseDir, inputFile, threadNumber, tf);
				ploid.useCache(useCache);
				ploid.sample(sampleNumber);
			}else{
				Walker rna = new RnaWalker(logdb, runName, baseDir, inputFile, threadNumber, tf);
				rna.useCache(useCache);
				rna.sample(sampleNumber);
			}
		}else{
			if(nargs.contains("--ploidy")){
				Walker ploid = new LargeDnaWalker(logdb, runName, baseDir, inputFile, threadNumber, tf);
				ploid.useCache(useCache);
				ploid.sample(sampleNumber);
			}else{
				Walker dna = new DnaWalker(logdb, runName, baseDir, inputFile, threadNumber, tf);
				dna.useCache(useCache);
				dna.sample(sampleNumber);
			}
		}

	}
	
	private static void showHelp(String error){
		println("WorkflowWalker");
		println("Version "+VERSION);
		
		println("\n Command args:");
		println("\t-h (--help) to call this menu");
		println("\t-d (--database) <name of the database>");
		println("\t-i (--input) <inputfile> for the path to the input file");
		println("\t-b (--base) <path> to set the working path (default is '"+baseDir+"')");
		println("\t-r (--run) <name> to set the name of the run (default is '"+runName+"')");
		println("\t-s (--sample) <number> to set the number of samples (default is "+sampleNumber+")");
		println("\t--no-cache to deactivate the cache function (will not use old results for new pipelines)");
		println("\t-t (--thread) <number> to set the number of available threads (default is "+threadNumber+")");
		println("\t--gold changes the target function from meta comparison (default) to comparison with a given gold standard");
		println("\t--overwrite defines if any old variants from previous runs will be overwritten (default: off)");
		println("\t--rna to use the GATK RNAseq workflow instead of the GATK DNA Variant Calling workflow.");
		println("\t--anno to annotate the raw variants in the last step.");
		println("\t-seed <number> to set the random seed. Default is "+randomSeed);
		
		println("\nRemember that the working path must be the parent directory of the following folders:");
		println("\tinputs/");
		println("\tcache/ (will be created if not exists)\n");
		
		if(!error.isEmpty()){
			logger.log(Level.SEVERE, error);
		}
		
		System.exit(0);
	}
	
	private static void println(String s){
		System.out.println(s);
	}

	/**
	 * Used to enable and configure logging for the run
	 */
	private static void startLogger(){

		try{
			LogManager.getLogManager().readConfiguration();
			logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
			//remove all old handlers (not the console handler) to cancel writing another logfile to ~/
			for(Handler old : logger.getHandlers()){
				if(!(old instanceof ConsoleHandler)){
					logger.removeHandler(old);
				}
			}
			new File("logs").mkdirs();
			Handler handler = new FileHandler( "logs/run."+(System.currentTimeMillis()/1000)+".log" );
			handler.setFormatter(new SimpleFormatter());
			logger.addHandler( handler );
		}catch(IOException ioe){
			System.out.println("Unknown error while creating the log.");
			ioe.printStackTrace();
			System.exit(ExitCode.UNKNOWNERROR);
		}
		logger.finer("Logger created.");
		
	}
	
}
