package com.fangyuzhong.person;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by fangyuzhong on 17-6-12.
 */
public class PersonBusinessReduce extends Reducer<Text,PersonBusiness,Text,PersonBusiness>
{

    public static final Log LOG = LogFactory.getLog(PersonBusinessReduce.class);
    private PersonBusiness result = new PersonBusiness();
    /**
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key,Iterable<PersonBusiness> values,Context context)
        throws IOException,InterruptedException
    {
        result.setPersonCount(0);
        result.setPersonName(null);
        result.setPersonVistTimes(null);
        result.setVisthotlTimes(null);
        String strVistTimes="";
        int sumCount = 0;
        //LOG.info(key);
        for(PersonBusiness val:values)
        {

            //LOG.info(val);
            strVistTimes += val.getVisthotlTimes()+",";
            sumCount +=val.getPersonCount();
            result.setPersonName(val.getPersonName());
        }
        if(strVistTimes.length()!=0)
            strVistTimes = strVistTimes.substring(0,strVistTimes.length()-1);
        result.setPersonCount(sumCount);
        result.setVisthotlTimes(strVistTimes);
        context.write(key,result);
    }
}
