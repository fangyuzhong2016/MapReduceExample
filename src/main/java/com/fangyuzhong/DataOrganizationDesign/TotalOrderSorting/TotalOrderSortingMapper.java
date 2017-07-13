package com.fangyuzhong.DataOrganizationDesign.TotalOrderSorting;

import com.fangyuzhong.person.PersonBusinessMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * Created by fangyuzhong on 17-7-13.
 */
public class TotalOrderSortingMapper extends Mapper<Object, Text, Text, Text>
{
    private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final Log LOG = LogFactory.getLog(PersonBusinessMapper.class);

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
        String[] valueSplits = value.toString().split(",");
        if (valueSplits.length >= 7)
        {
            String strDate = valueSplits[0];
            context.write(new Text(strDate), value);
        }
    }
}
