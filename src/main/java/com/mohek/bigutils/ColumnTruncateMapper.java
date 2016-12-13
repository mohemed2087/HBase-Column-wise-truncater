package com.mohek.bigutils;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;

public class ColumnTruncateMapper extends TableMapper<NullWritable, NullWritable>{

    private HTable myTable;
    private ArrayList<Delete> deleteList = new ArrayList<Delete>();
    final private int buffer = 10000; /* Buffer size, tune it as desired */
    private String columnFamily = "";
    private String columnName = "";

    protected void setup(Context context) throws IOException, InterruptedException {
        /* HTable instance for deletes */
        myTable = new HTable(HBaseConfiguration.create(), context.getConfiguration().get("tableName").getBytes());
        columnFamily= context.getConfiguration().get("cf");
        columnName = context.getConfiguration().get("column");
        
    }

    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        deleteList.add(new Delete(row.get()).deleteColumn(columnFamily.getBytes(), columnName.getBytes())); /* Add delete to the batch */
        if (deleteList.size()==buffer) {
            myTable.delete(deleteList); /* Submit batch */
            deleteList.clear(); /* Clear batch */
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (deleteList.size()>0) {
            myTable.delete(deleteList); /* Submit remaining batch */
        }
        myTable.close(); /* Close table */
    }

}