package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file.TraceFileReaderFoundation;


public class EODTraceReader extends TraceFileReaderFoundation {   



	private String line;


	public EODTraceReader(String fileName, int from, int to, boolean furtherjobs, Class<? extends Job> jobType)
	
		throws SecurityException, NoSuchMethodException {
		super("log Format", fileName, from, to, furtherjobs, jobType);
	}
	
	//Creating Jobs

	protected Job createJobFromLine(String job)
	
			throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {

		line = null;
		final String[] lineData = line.split(" "); 
		try {
			
			// Declare 
			
			int jobState = Integer.parseInt(lineData[0]);   //job id string
			int procs = 1; 								   // allocated processors
			long submit = Long.parseLong(lineData[1]);    // submit time in seconds
			long waitTime = 0; 							 // waiting time in seconds
			long runTime = 400; // running time in seconds
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
			
		} 		// Catch invalid lines with ArrayIndexOutOfBoundsException
				catch (ArrayIndexOutOfBoundsException e) {
					// Line is null
				return null;
				}
		return null;
				
		}
			
			protected void metaDataCollector(String data) {
			
			}
	
			protected boolean isTraceLine(String line) throws ArrayIndexOutOfBoundsException {
				
				String[] lineData; // string array.
				
				// Check if the line is null.
				if(line != null) {
					// If Not null,  proceed to add to array.
					lineData = line.split(" ");
				} else {
					// if invalid.
					return false;
				}
				
				// Catch invalid lines with ArrayIndexOutOfBoundsException
				try {
					// Check for Job Arrival Timings 
					try {
						Integer.parseInt(lineData[0]);
					} catch (NumberFormatException e) {
						
						return false;
					}
					
					// Check for Job Duration
					try {
						Float.parseFloat(lineData[1]);
					} catch (NumberFormatException e) {
					}
						// If line contains any incorrect Data.
						return false;
					}
					
					// Job ID String
					if(lineData[2].isEmpty()) {
						
						return false;
					}
					
					// Check for Job Exec 
					if(!lineData[3].isEmpty()) {
						//  If Not empty
						if(!lineData[3].equals("url") && !lineData[3].equals("default") && !lineData[3].equals("export")) {
							// Does not match any job types listed (url, default, export) Returning FALSE.
							return false;
						}
					} else {
						
						return false;
					}
					
					// If all checks are passed then return true.
					return true;
					
				} catch(ArrayIndexOutOfBoundsException e) {
					// If checks are failed - return False.
					return false;
				}
				
			}
	
