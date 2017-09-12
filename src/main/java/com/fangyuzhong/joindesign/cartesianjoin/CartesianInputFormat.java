package com.fangyuzhong.joindesign.cartesianjoin;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * 自定义输入格式类
 * Created by fangyuzhong on 17-9-12.
 */
public class CartesianInputFormat extends FileInputFormat
{

    public static final String LEFT_INPUT_FORMAT = "cart.left.inputformat";
    public static final String LEFT_INPUT_PATH = "cart.left.path";
    public static final String RIGHT_INPUT_FORMAT = "cart.right.inputformat";
    public static final String RIGHT_INPUT_PATH = "cart.right.path";

    public static void setLeftInputInfo(JobConf conf,
                                        Class<? extends FileInputFormat> inputFormat, String inputPath)
    {
        conf.set(LEFT_INPUT_FORMAT, inputFormat.getCanonicalName());
        conf.set(LEFT_INPUT_PATH, inputPath);
    }

    public static void setRightInputInfo(JobConf job,
                                         Class<? extends FileInputFormat> inputFormat, String inputPath)
    {
        job.set(RIGHT_INPUT_FORMAT, inputFormat.getCanonicalName());
        job.set(RIGHT_INPUT_PATH, inputPath);
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int numSplits)
            throws IOException
    {

        try
        {
            // Get the input splits from both the left and right data sets
            InputSplit[] leftSplits = getInputSplits(conf,
                    conf.get(LEFT_INPUT_FORMAT), conf.get(LEFT_INPUT_PATH),
                    numSplits);
            InputSplit[] rightSplits = getInputSplits(conf,
                    conf.get(RIGHT_INPUT_FORMAT),
                    conf.get(RIGHT_INPUT_PATH), numSplits);

            // Create our CompositeInputSplits, size equal to left.length *
            // right.length
            CompositeInputSplit[] returnSplits = new CompositeInputSplit[leftSplits.length
                    * rightSplits.length];

            int i = 0;
            // For each of the left input splits
            for (InputSplit left : leftSplits)
            {
                // For each of the right input splits
                for (InputSplit right : rightSplits)
                {
                    // Create a new composite input split composing of the
                    // two
                    returnSplits[i] = new CompositeInputSplit(2);
                    returnSplits[i].add(left);
                    returnSplits[i].add(right);
                    ++i;
                }
            }

            // Return the composite splits
            LOG.info("Total splits to process: " + returnSplits.length);
            return returnSplits;
        } catch (ClassNotFoundException e)
        {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    @Override
    public RecordReader getRecordReader(InputSplit split, JobConf conf,
                                        Reporter reporter) throws IOException
    {
        // create a new instance of the Cartesian record reader
        return new CartesianRecordReader((CompositeInputSplit) split, conf,
                reporter);
    }

    private InputSplit[] getInputSplits(JobConf conf,
                                        String inputFormatClass, String inputPath, int numSplits)
            throws ClassNotFoundException, IOException
    {
        // Create a new instance of the input format
        FileInputFormat inputFormat = (FileInputFormat) ReflectionUtils
                .newInstance(Class.forName(inputFormatClass), conf);

        // Set the input path for the left data set
        inputFormat.setInputPaths(conf, inputPath);

        // Get the left input splits
        return inputFormat.getSplits(conf, numSplits);
    }
}