package com.mohek.bigutils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;


public class ColumnTruncateDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("tableName",args[0]);
		config.set("cf",args[1]);
		config.set("column",args[2]);
	    Job job = new Job(config);
	    job.setJarByClass(ColumnTruncateMapper.class);     

	    Scan scan = new Scan();
	    /*scan.setCaching(500);        
	    scan.setCacheBlocks(false);*/

	    TableMapReduceUtil.initTableMapperJob(args[0], scan,ColumnTruncateMapper.class,null, null, job);

	   job.setOutputFormatClass(NullOutputFormat.class);
	    job.setNumReduceTasks(0);

	    boolean b = job.waitForCompletion(true);
	    if (!b) {
	        throw new IOException("error with job!");
	    }
		return 0;
		
		
	}

	public static void main(String[] args) throws Exception {
		ColumnTruncateDriver runJob = new ColumnTruncateDriver();
		runJob.run(args);
	}
}
