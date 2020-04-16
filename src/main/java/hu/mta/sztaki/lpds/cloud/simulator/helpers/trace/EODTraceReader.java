package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file.TraceFileReaderFoundation;


public class EODTraceReader extends TraceFileReaderFoundation {   



	private String line;


	public EODTraceReader(String fileName, int from, int to, boolean furtherjobs, Class<? extends Job> jobType)
	
		throws SecurityException, NoSuchMethodException {
		super("log Format", fileName, from, to, furtherjobs, jobType);
	}
	
	//Create Job

	protected Job createJobFromLine(String job)
	
			throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {

		line = null;
		final String[] lineData = line.split(" "); 
		try {
			
			
			int jobState = Integer.parseInt(lineData[0]);   //job id string
			int procs = 1; 								   // allocated processors
			long submit = Long.parseLong(lineData[1]);    // submit time in seconds
			long waitTime = 0; 							 // waiting time in seconds
			long runTime = Long.parseLong(lineData[3]); // running time in seconds
			String user = null; 					   // userID
			String group = null;                      // groupID
			String exec = null;                      // execID
			if (jobState != 1 && (procs < 1 || runTime < 0)) {
				return null;
				
			} else {
				
				final String preceedingJobId = lineData[1].trim();
				if (!preceedingJobId.equals("-1")) {
					return jobCreator.newInstance(jobState, procs, submit, waitTime, runTime, user, group, exec); 
				}
				
				
				}
			
		}
				catch (ArrayIndexOutOfBoundsException e) {
					// Line is null
				return null;
				}
		return null;
				
		}
			
			
			protected void metaDataCollector(String data) {
			
			}

			
			protected boolean isTraceLine(String line) throws ArrayIndexOutOfBoundsException {
				
				String[] lineData; // Instantiate string array.
				
				// Check if the line argument is null.
				if(line != null) {
					// Not null, add its contents to the array.
					lineData = line.split(" ");
				} else {
					// Line is null, invalid.
					return false;
				}
				
				// Catch invalid lines with ArrayIndexOutOfBoundsException
				try {
					// Check for Job Arrival Time Integer
					try {
						Integer.parseInt(lineData[0]);
					} catch (NumberFormatException e) {
						// Data can't be parsed as an integer, data line contains incorrect data.
						return false;
					}
					
					// Check for Job Duration Float
					try {
						Float.parseFloat(lineData[1]);
					} catch (NumberFormatException e) {
						// Data can't be parsed as a float, data line contains incorrect data.
						return false;
					}
					
					// Check for Job ID String ( Not Whitespace )
					if(lineData[2].isEmpty()) {
						
						return false;
					}
					
					// Check for Job Executable String ( "url", "default" or "export" )
					if(!lineData[3].isEmpty()) {
						// Not blank
						if(!lineData[3].equals("url") && !lineData[3].equals("default") && !lineData[3].equals("export")) {
							// Doesn't equal any of the specified job executable types
							return false;
						}
					} else {
						
						return false;
					}
					
					// Passed all of the checks, return true.
					return true;
					
				} catch(ArrayIndexOutOfBoundsException e) {
					// Incorrect amount of arguments.
					return false;
				}
				
			}
	
