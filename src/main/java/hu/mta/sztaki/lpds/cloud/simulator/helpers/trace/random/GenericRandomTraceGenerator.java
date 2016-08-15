/*
 *  ========================================================================
 *  Helper classes to support simulations of large scale distributed systems
 *  ========================================================================
 *  
 *  This file is part of DistSysJavaHelpers.
 *  
 *    DistSysJavaHelpers is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *   DistSysJavaHelpers is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 *  (C) Copyright 2016, Gabor Kecskemeti (g.kecskemeti@ljmu.ac.uk)
 *  (C) Copyright 2012-2015, Gabor Kecskemeti (kecskemeti.gabor@sztaki.mta.hu)
 */

package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.random;

import java.util.Calendar;
import java.util.List;
import java.util.Random;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.TraceManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.TraceProducerFoundation;

/**
 * Foundation for random generated traces
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2016"
 * @author "Gabor Kecskemeti, Laboratory of Parallel and Distributed Systems,
 *         MTA SZTAKI (c) 2015"
 */
public abstract class GenericRandomTraceGenerator extends TraceProducerFoundation {

	public static final int defaultSeed = 1;

	/**
	 * To influence the random behavior of this class one is allowed to replace
	 * the generator any time with other Random implementations or with other
	 * generators with a different seed.
	 */
	public static Random r = new Random(defaultSeed);

	/**
	 * The list of currently generated jobs. (this list gets overwritten if a
	 * new trace generation is initiated with the generateJobs function.
	 */
	private List<Job> currentlyGenerated;

	/**
	 * Shows the position from which the getJobs function can collect the jobs
	 * of the currentlyGenerated list.
	 */
	private int jobIndex = -1;

	/**
	 * Basic trace characteristics
	 */
	private int jobNum = -1, maxTotalProcs = -1;

	/**
	 * The total number of jobs to be generated in a single run. This is the
	 * maximum length of the currentlyGenerated list.
	 * 
	 * @return the current number of jobs.
	 */
	public int getJobNum() {
		return jobNum;
	}

	/**
	 * Allows a different number of jobs to be generated in a single run.
	 * 
	 * Setting this value to negative allows concurrent modifications of jobNum
	 * and maxtotalprocs, i.e., the setting of the other will not take effect
	 * until you set this back to something positive.
	 * 
	 * @param jobNum
	 *            the new number of jobs for the length of the
	 *            currentlyGenerated list.
	 */
	public void setJobNum(int jobNum) {
		this.jobNum = jobNum;
		try {
			regenJobs();
		} catch (TraceManagementException e) {
			// Ignore.
		}
	}

	/**
	 * Determines the total number of processors available in the particular
	 * infrastructure. This is in fact used to determine the maximum number of
	 * processors used in parallel by all the jobs in a parallel section
	 * generated by this trace producer.
	 * 
	 * Note: if this value is bigger than the number of processors available in
	 * the simulated distributed infrastructure, then the generated trace will
	 * be useful for evaluating under-provisioning situations.
	 * 
	 * @return Maximum processor count in a parallel section.
	 */
	public int getMaxTotalProcs() {
		return maxTotalProcs;
	}

	/**
	 * Sets the total processors available for a parallel section. For details,
	 * see the documentation of getMaxTotalProcs().
	 * 
	 * Setting this value to negative allows concurrent modifications of jobNum
	 * and maxtotalprocs, i.e., the setting of the other will not take effect
	 * until you set this back to something positive.
	 * 
	 * @param maxTotalProcs
	 *            a new maximum processor count in the later generated parallel
	 *            sections.
	 */
	public void setMaxTotalProcs(int maxTotalProcs) {
		this.maxTotalProcs = maxTotalProcs;
		try {
			regenJobs();
		} catch (TraceManagementException e) {
			// Ignore.
		}
	}

	/**
	 * To be used by the setter functions. Determines if the currentlyGenerated
	 * list of jobs got obsolete. If they did then it asks for their
	 * regeneration with the new trace characteristics.
	 */
	final protected void regenJobs() throws TraceManagementException {
		if (isPrepared()) {
			try {
				if (jobIndex < 0 || jobIndex >= currentlyGenerated.size()) {
					System.err.println("Random trace generation starts at " + Calendar.getInstance().getTime());
					currentlyGenerated = generateJobs();
					System.err.println("Random trace generation stops at " + Calendar.getInstance().getTime());
					jobIndex = 0;
				}
			} catch (TraceManagementException e) {
				jobIndex = -1;
				currentlyGenerated = null;
				throw e;
			}
		}
	}

	/**
	 * Generates a new set of jobs then return the complete set immediately to
	 * the caller. The generation is done with the generateJobs function.
	 * 
	 * @return a completely new trace with the length of jobNum.
	 * @throws TraceManagementException
	 */
	@Override
	public List<Job> getAllJobs() throws TraceManagementException {
		regenJobs();
		if (currentlyGenerated != null) {
			jobIndex = currentlyGenerated.size();
		}
		return currentlyGenerated;
	}

	/**
	 * Produces a trace with the length of 'num'. Trace generation only happens
	 * if the currentlyGenerated list does not contain enough entries. In such
	 * case first the previously generated sublist is served, then a new set is
	 * generated and added as its tail. The function recursively invokes itself
	 * until there are enough elements produced for a trace with the lenght of
	 * 'num'.
	 * 
	 * @param num
	 *            The number of trace elements expected in the returning list.
	 * 
	 * @return a trace list with the length of num
	 */
	@Override
	public List<Job> getJobs(int num) throws TraceManagementException {
		if (currentlyGenerated == null) {
			regenJobs();
			if (currentlyGenerated == null) {
				throw new RuntimeException("Tere were no new jobs generated!");
			}
		}
		if (jobIndex + num < currentlyGenerated.size()) {
			List<Job> listPart = currentlyGenerated.subList(jobIndex, jobIndex + num);
			jobIndex += num;
			return listPart;
		} else {
			List<Job> firstPart = currentlyGenerated.subList(jobIndex, currentlyGenerated.size());
			num -= currentlyGenerated.size() - jobIndex;
			regenJobs();
			if (currentlyGenerated == null) {
				throw new RuntimeException("Tere were no new jobs generated!");
			}
			List<Job> secondPart = getJobs(num);
			firstPart.addAll(secondPart);
			return secondPart;
		}
	}

	/**
	 * Determines if the trace generator has been set up correctly and it is
	 * ready to produce new trace entries
	 * 
	 * HINT: this function should be extended if a subclass has more
	 * requirements about correct setup
	 * 
	 * @return <i>true</i> if the trace generation can start any time
	 */
	protected boolean isPrepared() {
		return jobNum >= 0 && maxTotalProcs >= 0;
	}

	/**
	 * Constructor to pass on the job type to the TraceProducerFoundation
	 * 
	 * @param jobType
	 *            the kind of job this generator should produce
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 */
	public GenericRandomTraceGenerator(final Class<? extends Job> jobType)
			throws NoSuchMethodException, SecurityException {
		super(jobType);
	}

	/**
	 * This function should be implemented by subclasses and it is intended to
	 * produce a trace according to the generator's set up if the isPrepared()
	 * function returns true.
	 * 
	 * @return the generated trace
	 * @throws TraceManagementException
	 *             if the trace cannot be generated because of some internal
	 *             issue (most likely the use of reflection could be the issue
	 *             inside the TraceProducerFoundation).
	 */
	abstract protected List<Job> generateJobs() throws TraceManagementException;

}
