package com.fangyuzhong.dataorganizationdesign.TotalOrderSorting;

import com.fangyuzhong.utility.MRDPUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

/**
 * Created by fangyuzhong on 17-7-13.
 */
public class AnonymizeMapper extends Mapper<Object, Text, IntWritable, Text>
{

    private IntWritable outkey = new IntWritable();
    private Random rndm = new Random();
    private Text outvalue = new Text();

    /**
     * map函数
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
    {

        // Parse the input string into a nice map
        Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
                .toString());

        if (parsed.size() > 0)
        {
            StringBuilder bldr = new StringBuilder();
            bldr.append("<row ");
            for (Map.Entry<String, String> entry : parsed.entrySet())
            {

                if (entry.getKey().equals("UserId")
                        || entry.getKey().equals("Id"))
                {
                    // ignore these fields
                } else if (entry.getKey().equals("CreationDate"))
                {
                    // Strip out the time, anything after the 'T' in the
                    // value
                    bldr.append(entry.getKey()
                            + "=\""
                            + entry.getValue().substring(0,
                            entry.getValue().indexOf('T')) + "\" ");
                } else
                {
                    // Otherwise, output this.
                    bldr.append(entry.getKey() + "=\"" + entry.getValue()
                            + "\" ");
                }

            }
            bldr.append(">");
            outkey.set(rndm.nextInt());
            outvalue.set(bldr.toString());
            context.write(outkey, outvalue);
        }
    }

}
