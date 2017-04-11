package instances;

import general.ExitCode;
import general.FileHandler;
import general.Input;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.*;

import logdb.LogDB;
import sampler.Edge;
import sampler.ExitCodeException;
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

abstract public class BashWalker extends Walker {
	/** Used for logging runtime information */
	private boolean SINGLE = false;
	private Map<String, Input> inputFiles = new HashMap<>();
	private String baseDir;
	private String cacheDir;
	protected int THREADS = 1; //Default
	protected TargetFunction tf;
	
	/**
	 * 
	 * @param logdb An instance of the LogDB
	 * @param runName The name of the current sampling process
	 * @param basedir path to the base directory
	 * @param inputFile Path to the input file
	 * @param threads Number of threads used
	 * @param target The target function instance
	 */
	public BashWalker(LogDB logdb, String runName, String basedir, String inputFile, TargetFunction target, int threads) {
		super(logdb, runName);
		logger.fine("Created new Bash Walker instance");
		this.THREADS = threads;
		if(target == null){
			logger.warning("Given target function was null, creating a default target function with meta comparison and a burryIn phase of 20 samples.");
			tf = new TargetFunction(logdb);
		}else{
			tf = target;
		}
		
		if(basedir.endsWith("/")){
			logger.finer("Path '"+basedir+"' ended with a slash, stripping it for uniformity");
			basedir = basedir.substring(0, basedir.length()-1);
		}

		inputFiles = FileHandler.getInput(basedir, inputFile, getInputKeys());
		SINGLE = !(inputFiles.containsKey("fq2"));
		this.logdb.prepareRun(this.getSteps().size(), runName, true, (tf.getTarget() == TargetFunction.SIMILARITYgoldstandard));
		
		if(tf.getTarget() == TargetFunction.SIMILARITYgoldstandard){
			if(inputFiles.containsKey("gold")){
				tf.setGoldstandard(basedir+inputFiles.get("gold").getFullInput(), runName);
			}else{
				logger.warning("TargetFunction is set to compare to a gold standard, but none was found in the input keys.");
			}
		}
		
		createCache(basedir);
		
	}

	/**
	 * Creates the directory where all workfiles will be stored
	 * @param baseDir
	 */
	private void createCache(String baseDir){
		this.baseDir = baseDir;
		this.cacheDir = FileHandler.createCache(baseDir, runName);
	}
	
	protected boolean isSingle() {
		return this.SINGLE;
	}
	
	/**
	 * @return Returns a string array containing the key names of all elements that are needed in the input file
	 */
	abstract protected String[] getInputKeys();

	/**
	 * Returns the execDir for the given configId
	 * @param configId The id of the configuration
	 * @return Returns the path to the config directory for the given configuration
	 */
	protected String getExecDir(long configId){
		return ( this.cacheDir + "/conf"+configId );
	}
	
	@Override
	protected void createExecutionEnv(long configId) {
		String exeDir = getExecDir(configId);
		if(Files.notExists(Paths.get(exeDir))){
			// try to create the path
			try{
		        new File(exeDir).mkdir();
		        if(Files.notExists(Paths.get(exeDir))){
		        	logger.warning("Path '"+exeDir+"' could not be created.");
		        	System.exit(ExitCode.PATHERROR);
		        }
		    } catch(SecurityException se) {
		        logger.log(Level.SEVERE, "Error while trying to create the executing directory", se);
		        System.exit(ExitCode.PATHERROR);
		    }   
		}else{
			logger.warning("Directory '"+exeDir+"' already existed.");
		}
	}

	@Override
	protected void handleInputFiles(long configId) throws ExitCodeException {
		String execDir = getExecDir(configId);
		Map<String, Input> fileStack = new HashMap<>();
		fileStack.putAll(inputFiles);
		
		//TODO: add mkdir option to process call
		// link the initial files that are needed for each run anyways
		Process p;
		for(String key : fileStack.keySet()){
			//create process and set working dir
			File f = new File(this.baseDir +  fileStack.get(key).getFullInput());
			if(!f.exists()) { 
				logger.log(Level.FINER, "Assuming "+key+" to be a string variable, not creating a soft link.");
				continue;
			}
			ProcessBuilder pb = new ProcessBuilder("ln", "-snf", baseDir +  fileStack.get(key).getFullInput(), execDir + "/" + fileStack.get(key).getFileOnly());
			logger.finest("Command: ln -snf "+baseDir+fileStack.get(key).getFullInput()+" "+execDir + "/" + fileStack.get(key).getFileOnly());
			try{
				p = pb.start();
				int exitValue = p.waitFor();
				if(exitValue != 0){
					logger.severe("Exit value for linking "+key+"("+fileStack.get(key).getFullInput()+") is " + exitValue);
					throw new ExitCodeException(exitValue);
				}
			}catch(Exception ioe){
				logger.log(Level.SEVERE, "Error while copying the input files as links to the exec folder", ioe);
				throw new ExitCodeException(ExitCode.INPUTERROR);
			}
		}
		
	}
	
	@Override
	protected boolean handleCacheFiles(long configId, Edge[] workflow, long cacheId, int lastCommonStep) {
		if(!(USECACHE && lastCommonStep > 0 && cacheId > 0)){
			return false;
		}
		String execDir = getExecDir(configId);
		ArrayList<Input> fileStack = new ArrayList<>();
		//check the cache
		// assert the cache target folder exists
		String cachedConf = this.getExecDir(cacheId);
		if(Files.notExists(Paths.get(cachedConf))){
			logger.warning("Tried to resolve cache dir "+cachedConf+" but no directory found. Resuming normal execution.");
			return false;
		}else{
			logger.finest("Cache dir "+cachedConf+" exists, resuming cache handling...");
		}
		//add files from the cache to the input stack
		for(int i=0; i<lastCommonStep; i++){
			if(workflow[i].getOutputFiles() == null){
				//skip if step has no outputs
				continue;
			}
			for(String cacheFile : workflow[i].getOutputFiles()){
				Input val = new Input(cachedConf+"/"+cacheFile);
				val.setFileOnly(cacheFile);
				fileStack.add(val);
			}
		}
		Process p;
		for(Input inp : fileStack){
			File f = new File(execDir + "/" + inp.getFileOnly());
			String parent = f.getParent();
			//create process and set working dir
			ProcessBuilder pbMkdir = new ProcessBuilder("mkdir","-p",parent);
			pbMkdir.redirectErrorStream(true);
			ProcessBuilder pbLn = new ProcessBuilder("ln", "-snf", inp.getFullInput(), f.getAbsolutePath());
			pbLn.redirectErrorStream(true);
			logger.finest("Command: "+pbMkdir.command());
			logger.finest("Command: "+pbLn.command());
			try{
				p = pbMkdir.start();
				//add the output to the log
				BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
				String line;
				while ( (line = reader.readLine()) != null) {
				   logger.fine(line);
				}
				int exitValue = p.waitFor();
				if(exitValue != 0){
					logger.severe("Exit value for mkdir "+inp.getFileOnly()+" is " + exitValue);
					throw new Exception("Error while creating directory, task could not be executed.");
				}
				p = pbLn.start();
				//add the output to the log
				reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
				while ( (line = reader.readLine()) != null) {
				   logger.fine(line);
				}
				exitValue = p.waitFor();
				if(exitValue != 0){
					logger.severe("Exit value for linking "+inp.getFileOnly()+" is " + exitValue);
					return false;
				}
			}catch(Exception ioe){
				logger.log(Level.SEVERE, "Error while copying the cache files as links to the exec folder", ioe);
				return false;
			}
		}
		//all ok
		return true;
	}
	
	/**
	 * Returns the file name (without path) for an input file key
	 * @param name The key name
	 * @return The name of the corresponding file, without any path information, or the value of the variable if the key points to a variable
	 */
	protected String getFile(String name) {
		if(inputFiles.get(name) == null){
			logger.warning("Requested file or variable for key '"+name+"' does not exist.");
		}
		logger.finest("Requested file for key '"+name+"' = "+inputFiles.get(name).getFileOnly());
		return inputFiles.get(name).getFileOnly();
	}
	
	@Override
	protected void deleteWorkfiles(long configId){
		String execDir = getExecDir(configId);
		logger.info("Deleting "+execDir+"...");
		Process p;
		ProcessBuilder rm = new ProcessBuilder("rm", "-r", execDir);
		rm.redirectErrorStream(true);
		logger.finest("Command: "+rm.command());
		try{
			p = rm.start();
			//add the output to the log
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ( (line = reader.readLine()) != null) {
			   logger.fine(line);
			}
			int exitValue = p.waitFor();
			if(exitValue != 0){
				logger.severe("Exit value for rm "+execDir+" is: " + exitValue);
				throw new Exception("Error while removing directory, task could not be executed.");
			}
		}catch(Exception ioe){
			logger.log(Level.SEVERE, "Error while removing the cache files", ioe);
		}
	}

	@Override
	protected void traverseEdge(Edge e, long configId) throws ExitCodeException {
		StringBuilder script = new StringBuilder("#!/bin/bash");
		script.append(System.getProperty("line.separator"));
		script.append(e.getCommand());
		String scriptname = e.getGroupName() + ".sh";
		
		logger.fine("Script: "+script.toString()+System.getProperty("line.separator"));
		
		//save the new file into the folder as #name.sh
		File nuscript = new File( this.getExecDir(configId) + "/" + scriptname);
		try{
			PrintWriter writer = new PrintWriter(nuscript, "UTF-8");
			writer.print(script.toString());
			writer.close();
			//make executable for all
			Process chmod = Runtime.getRuntime().exec(new String[]{"chmod", "777", nuscript.getAbsolutePath()});
			int exit = chmod.waitFor();
		        logger.fine("Changed the permissions of "+nuscript.getAbsolutePath()+" to 777, exit value: "+exit);
		}catch(Exception ex){
			logger.log(Level.SEVERE,"Exception while writing the script file for task "+e.getGroupName(), ex);
			throw new ExitCodeException(ExitCode.PATHERROR);
		}
		//create process and set working dir
		Process p;
		ProcessBuilder pb;
		pb = new ProcessBuilder("./"+scriptname);
		pb.directory(new File(getExecDir(configId)));
		pb.redirectErrorStream(true);
		//pb.inheritIO(); // print to stdout of the java process
		
		//Assert that all inputs are located
		logger.finest("Detected "+e.getInputFiles().length+" inputs, validating their existence...");
		for(int i=0; i< e.getInputFiles().length; i++){
			File f = new File(getExecDir(configId)+"/"+e.getInputFiles()[i]);
			if(!f.exists()){
				logger.log(Level.SEVERE, "File assertion failed for input "+i+": "+getExecDir(configId)+"/"+e.getInputFiles()[i]+" not found in the directory for task "+e.getGroupName());
				logger.info("Assuming '"+e.getInputFiles()[i]+"' is a string variable. If this is not true, a problem occured.");
			}else{
				logger.finer("File "+i+": "+e.getInputFiles()[i]+" exists.");
			}
		}
		//run the task and catch the exit code
		int exitValue = 0;
		try{
			Logger tasklog = startLogger(e.getGroupName(), configId);
			p = pb.start();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ( (line = reader.readLine()) != null) {
				tasklog.info(line);
			}
			exitValue = p.waitFor();
			logger.info("Exit value of task "+e.getGroupName()+" execute is " + exitValue);
			tasklog.info("Exit value of task "+e.getGroupName()+" execute is " + exitValue);
		}catch(IOException ioe){
			logger.log(Level.SEVERE,"IOException while executing the script file for task "+e.getGroupName(), ioe);
			throw new ExitCodeException(ExitCode.EXECUTEERROR);
		} catch (InterruptedException ie) {
			logger.log(Level.SEVERE,"InterruptedException while executing the script file for task "+e.getGroupName(), ie);
			throw new ExitCodeException(ExitCode.EXECUTEERROR);
		}
		
		//if exit code is 0, assert that all output files exist
		if(exitValue == 0){
			if(e.getOutputFiles() != null){
				for(String s : e.getOutputFiles()){
					File f = new File(getExecDir(configId)+"/"+s);
					if(!f.exists()){
						logger.log(Level.SEVERE, "File assertion failed: output "+getExecDir(configId)+"/"+s+" not found in the directory for task "+e.getGroupName());
						throw new ExitCodeException(ExitCode.INPUTERROR);
					}
				}
			}
		}else{
			throw new ExitCodeException(exitValue);
		}
	}

	/**
	 * Used to enable and configure logging for the run
	 */
	private Logger startLogger(String taskname, long configId) throws IOException {

		LogManager.getLogManager().readConfiguration();
		Logger log = Logger.getLogger(Logger.class.getName());
		//remove all old handlers (not the console handler) to cancel writing another logfile to ~/
		for(Handler old : log.getHandlers()){
			if(!(old instanceof ConsoleHandler)){
				log.removeHandler(old);
			}
		}
		StringBuilder logfile = new StringBuilder(this.getExecDir(configId));
		logfile.append("/");
		logfile.append(taskname);
		logfile.append(".conf");
		logfile.append(configId);
		logfile.append(".log");
		Handler handler = new java.util.logging.FileHandler( logfile.toString());
		handler.setFormatter(new SimpleFormatter());
		log.addHandler( handler );

		return log;

	}
	
}
