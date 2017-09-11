package com.fangyuzhong.dataorganizationdesign.PartitionedDesign;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by fangyuzhong on 17-7-12.
 */
public class PartitionedUsers
{
    public static class LastAccessDateMapper extends
            Mapper<Object, Text, IntWritable, Text>
    {

        // 格式时间
        private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        private IntWritable outkey = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {

            String[] valueSplits = value.toString().split(",");
            if (valueSplits.length >= 7)
            {
                // Grab the last access date
                String strDate =valueSplits[5];

                // skip this record if date is null
                if (strDate != null)
                {
                    try
                    {
                        // Parse the string into a Calendar object
                        Calendar cal = Calendar.getInstance();
                        cal.setTime(frmt.parse(strDate));
                        outkey.set(cal.get(Calendar.YEAR));
                        // Write out the year with the input value
                        context.write(outkey, value);
                    } catch (ParseException e)
                    {
                        // An error occurred parsing the creation Date string
                        // skip this record
                    }
                }
            }
        }
    }

    public static class LastAccessDatePartitioner extends
            Partitioner<IntWritable, Text> implements Configurable
    {

        private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";

        private Configuration conf = null;
        private int minLastAccessDateYear = 0;

        @Override
        public int getPartition(IntWritable key, Text value, int numPartitions)
        {
            return key.get() - minLastAccessDateYear;
        }

        @Override
        public Configuration getConf()
        {
            return conf;
        }

        @Override
        public void setConf(Configuration conf)
        {
            this.conf = conf;
            minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
        }

        /**
         * Sets the minimum possible last access date to subtract from each key
         * to be partitioned<br>
         * <br>
         * <p>
         * That is, if the last min access date is "2008" and the key to
         * partition is "2009", it will go to partition 2009 - 2008 = 1
         *
         * @param job                   The job to configure
         * @param minLastAccessDateYear The minimum access date.
         */
        public static void setMinLastAccessDate(Job job,
                                                int minLastAccessDateYear)
        {
            job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR,
                    minLastAccessDateYear);
        }
    }

    /**
     *
     */
    public static class ValueReducer extends
            Reducer<IntWritable, Text, Text, NullWritable>
    {

        protected void reduce(IntWritable key, Iterable<Text> values,
                              Mapper.Context context) throws IOException, InterruptedException
        {
            for (Text t : values)
            {
                context.write(t, NullWritable.get());
            }
        }
    }

}
