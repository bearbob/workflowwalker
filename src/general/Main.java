package general;

import instances.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.*;
import java.util.logging.FileHandler;

import logdb.LogDB;
import sampler.Walker;

/*
		% WorkflowWalker: A Workflow Parameter Optimizer
		%
		% Copyright 2017 Björn Groß and Raik Otto
		%
		% Licensed under the Apache License, Version 2.0 (the "License");
		% you may not use this file except in compliance with the License.
		% You may obtain a copy of the License at
		%
		%    http://www.apache.org/licenses/LICENSE-2.0
		%
		% Unless required by applicable law or agreed to in writing, software
		% distributed under the License is distributed on an "AS IS" BASIS,
		% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
		% See the License for the specific language governing permissions and
		% limitations under the License.
*/

public class Main {
	private static Logger logger;
	private static final String VERSION = "1.47b";
	//defaults
	private static String runName = "dna";
	private static String inputFile = "";
	private static String baseDir = "~/workspace";
	private static String dbname = "log.db"; //default name
	private static int sampleNumber = 100;
	private static int threadNumber = 1;
	private static int randomSeed;
	
	
	public static void main(String[] args) {
		//generate a random int as seed, that can be replaced later on if the user wished to do so
		Random rand = new Random();
		randomSeed = rand.nextInt();
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


		String[][] sparameter = {
				new String[]{"Run name", "r", "run"},
				new String[]{"Database name", "d", "database"},
				new String[]{"Input file", "i", "input"},
				new String[]{"Base path", "b", "base"}
		};
		String[] pstring = {runName, dbname, inputFile, baseDir};
		for(int i=0; i<sparameter.length; i++) {
			String pattern1 = "-"+sparameter[i][1];
			String pattern2 = "--"+sparameter[i][2];
			if (nargs.contains(pattern1) || nargs.contains(pattern2)) {
				int pos = Math.max(nargs.indexOf(pattern1), nargs.indexOf(pattern2));
				logger.finest("Position for "+sparameter[i][0]+" " + pos);
				try {
					String val = nargs.get(pos + 1);
					if (val.startsWith("-")) {
						showHelp("Value expected after "+sparameter[i][0]+" option but next option found instead: " + val);
					}
					pstring[i] = val;
					logger.finest(sparameter[i][0]+" value: '" + pstring[i] + "'");
				} catch (IndexOutOfBoundsException ie) {
					showHelp("Argument expected after "+sparameter[i][0]+" option but none found.");
				}
			}
			logger.finest(sparameter[i][0]+" handled.");
		}
		runName = pstring[0];
		dbname = pstring[1];
		inputFile = pstring[2];
		baseDir = pstring[3];

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
					logger.finest(parameter[i][0]+" value: '" + pint[i] + "'");
				} catch (IndexOutOfBoundsException ie) {
					showHelp("Argument expected after "+parameter[i][0]+" option but none found.");
				}
			}
			logger.finest(parameter[i][0]+" handled.");
		}
		randomSeed = pint[0];
		threadNumber = pint[1];
		sampleNumber = pint[2];

		LogDB logdb = new LogDB(dbname, randomSeed);
		TargetFunction tf = new TargetFunction(logdb);

		//this is for testing the walker with a simple command
		if(nargs.contains("--test")){
			// RastriginWalker is a basic function to test the functionality
			Walker test = new RastriginWalker(logdb, runName);
			test.sample(sampleNumber);
			System.exit(0);
		}
		
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
		println("\t-seed <number> to set the random seed. Otherwise a random seed will be used.");

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
