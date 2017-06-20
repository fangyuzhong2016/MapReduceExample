package com.fangyuzhong.person;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

/**
 * Created by fangyuzhong on 17-6-2.
 */
public class PersonBusinessMapper extends Mapper<Object,Text,Text,PersonBusiness>
{
    private Text outId = new Text();
    private PersonBusiness personBusiness = new PersonBusiness();
    private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final Log LOG = LogFactory.getLog(PersonBusinessMapper.class);

    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException
    {
        String[] valueSplits = value.toString().split(",");
        LOG.info(valueSplits.length);
        if(valueSplits.length>=7)
        {
            String personName = valueSplits[0];
            String personId = valueSplits[1];
            String personVistTime = "";
            personVistTime = valueSplits[5];
            Date personVistDate = new Date();
            if (personVistTime != "")
            {
                try
                {
                    personVistDate = simpleDateFormat.parse(personVistTime);
                } catch (ParseException ex)
                {
                    LOG.fatal("时间转换错误", ex);
                }
            } else
            {
                personVistTime = personVistDate.toString();
            }
            personBusiness.setId(personId);
            personBusiness.setPersonName(personName);
            personBusiness.setVisthotlTimes(personVistTime);
            personBusiness.setPersonVistTimes(personVistDate);
            personBusiness.setPersonCount(1);
            outId.set(personId);
            context.write(outId, personBusiness);
        }
    }

}
