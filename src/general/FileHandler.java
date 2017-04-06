package general;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

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

public class FileHandler {
	private static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	
	public static Map<String, Input> getInput(String basedir, String inputFile, String[] keyCheck){
		Map<String, Input> inputFiles = new HashMap<>();
		String failedFiles = "";
		if(basedir.endsWith("/")){
			logger.fine("Path '"+basedir+"' ended with a slash, stripping it for uniformity");
			basedir = basedir.substring(0, basedir.length()-1);
		}
		//fill the map
		logger.finest("Filling the map with values from the input file.");
		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
		    String line;
		    read:while ((line = br.readLine()) != null) {
		       line = line.trim();
		       if(line.startsWith("%")) continue read; //skip comments
		       if(line.isEmpty()) continue read; //skip empty lines
		       if(!line.contains("=")) continue read; // skip lines without assignment
		       String[] l = line.split("=");
		       inputFiles.put(l[0].trim(), new Input("/inputs/" + l[1].trim()));
		       logger.fine("Added value '"+l[1].trim()+"' with key "+l[0].trim()+" to the map.");
		    }
		    //assert that all necessary files exist		    
		    boolean failed = false;
		    for(String s : keyCheck){
		    	if(!inputFiles.containsKey(s)){
		    		logger.log(Level.WARNING, "File/Variable assertion failed: needed key "+s+" not found in the input file.");
		    		//if the second read is missing, this might be on purpose because the first read is single-end
		    		if(s.equals("fq2")){
		    			logger.log(Level.INFO, "Assuming the f1 file to be single-end, if you used paired reads this is an error.\n Changing alignment for single reads...");
		    		}else{
		    			failedFiles += s+"; ";
		    			failed = true;
		    		}
		    	}
		    }//end key check
		    for(Map.Entry<String, Input> entry : inputFiles.entrySet()){
		    	logger.finest("Check if file for key "+entry.getKey()+" exists: "+basedir +entry.getValue().getFullInput());
	    		File f = new File(basedir + entry.getValue().getFullInput());
				if(!f.exists()) { 
					logger.log(Level.WARNING, "File assertion failed: "+basedir + entry.getValue().getFullInput()+" does not exist. Assuming "+entry.getKey()+" to be a string variable.");
					inputFiles.get(entry.getKey()).setVariable();
				}else{
					//file exists, save the shorter version to the map
					entry.getValue().setFileOnly(entry.getValue().getFullInput().substring(entry.getValue().getFullInput().lastIndexOf("/")+1));
				}
		    }
		    
		    if(failed){
		    	logger.log(Level.SEVERE, "Not all files needed for the workflow could be found in the input file. ("+failedFiles+")");
		    	System.exit(ExitCode.INPUTERROR);
		    }
		}catch(IOException ioe){
			logger.log(Level.SEVERE, "Problem while reading the input data file.", ioe);
			System.exit(ExitCode.PATHERROR);
		}catch(Exception ex){
			logger.log(Level.SEVERE, "Unknown error while checking the input data files", ex);
			System.exit(ExitCode.UNKNOWNERROR);
		}
		
		return inputFiles;
	}
	
	/**
	 * 
	 * @param based The path to the base directory as string
	 * @return The path to the cache directory
	 */
	public static String createCache(String based, String runName){
		if(based == null){
			logger.warning("BaseDir string was received by createCache but is null.");
			based = "";
		}
		String base = based;
		if(base.endsWith("/")){
			logger.fine("Path '"+base+"' ended with a slash, stripping it for uniformity");
			base = base.substring(0, base.length()-1);
		}
		base = base + "/cache/"+runName;
		
		if(Files.notExists(Paths.get(base))){
			logger.info("BaseDir '"+base+"' does not exist yet, will be created now.");
			// try to create the path
			try{
		        new File(base).mkdir();
		        if(Files.notExists(Paths.get(base))){
		        	logger.warning("Path '"+base+"' still not existing.");
		        	System.exit(ExitCode.PATHERROR);
		        }
		    } catch(SecurityException se) {
		        logger.log(Level.SEVERE, "Error while trying to create the working path", se);
		        System.exit(ExitCode.PATHERROR);
		    }   
		}else{
			logger.fine("BasePath '"+base+"' already exists.");
		}
		logger.finest("createCache() was successful");
		
		return base;
	}
	
}
