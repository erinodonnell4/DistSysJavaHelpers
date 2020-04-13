package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace;

import java.lang.reflect.InvocationTargetException;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file.TraceFileReaderFoundation;

public class EODTraceReader extends TraceFileReaderFoundation {   

	TraceFileReaderFoundation TFRF;
	
	//NEEDS FIXED

	public EODTraceReader(String fileName, int from, int to, boolean furtherjobs, Class<? extends Job> jobType)
			throws SecurityException, NoSuchMethodException {
		super("Grid workload format", fileName, from, to, furtherjobs, jobType);
		// TODO Auto-generated constructor stub
	}

	protected boolean isTraceLine(final String param) {
		boolean isValid = false;
		String[] test = param.split(" ");

		if (test[0].equals(test[0].valueOf(0))) {
			if (test[1].equals(test[1].valueOf(1))) {
				if (test[3].equalsIgnoreCase("default")) {
					isValid = true;
				} else if (test[3].equalsIgnoreCase("url")) {
					isValid = true;
				} else if (test[3].equalsIgnoreCase("export")) {
					isValid = true;

				}
			}
		}

		if (isValid = true) {
			return true;
		} else
			return false;

	}

	protected void metaDataCollector(String data) {

	}

	protected Job createJobFromLine(String job)
			throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {

		final String[] fragments = job.trim().split("\\s+");
		try {
			/**
			 * String id, long submit, long queue, long exec, int nprocs, double ppCpu, long
			 * ppMem, String user, String group, String executable, Job preceding, long
			 * delayAfter
			 */
			// 1 done, 0 fail, 5 cancel
			int jobState = Integer.parseInt(fragments[10]);
			int procs = Integer.parseInt(fragments[4]);
			long runtime = Long.parseLong(fragments[3]);
			long waitTime = Long.parseLong(fragments[2]);
			if (jobState != 1 && (procs < 1 || runtime < 0)) {
				return null;
			} else {
				final String preceedingJobId = fragments[16].trim();
				Job preceedingJob = null;
				if (!preceedingJobId.equals("-1")) {
					preceedingJob = jobLookupInCache(preceedingJobId);
				}
				return jobCreator.newInstance(
						// id:
						fragments[0],
						// submit time in secs:
						Long.parseLong(fragments[1]),
						// wait time in secs:
						Math.max(0, waitTime),
						// run time in secs:
						Math.max(0, runtime),
						// allocated processors:
						Math.max(1, procs),
						// average cpu time:
						(long) Double.parseDouble(fragments[5]),
						// average memory:
						Long.parseLong(fragments[6]),
						// userid:
						fragments[11],
						// groupid:
						fragments[12],
						// execid:
						fragments[13], preceedingJob, preceedingJob == null ? 0 : Long.parseLong(fragments[17]));
			}

		} //catch (ArrayIndexOutOfBoundsException ex) {
			// Ignore
		}
		return null;
	}
}