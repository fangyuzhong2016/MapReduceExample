package com.fangyuzhong.FilterDesign.TopFilterN;

import com.fangyuzhong.person.PersonBusinessMapper;
import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ID;
import org.apache.hadoop.mapreduce.Mapper;

import javax.jws.soap.SOAPBinding;
import java.io.IOException;
import java.io.StringReader;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by fangyuzhong on 17-7-4.
 */
public class DistinctUserMapper extends Mapper<Object, Text, Text,User>
{

    private Text outID=new Text();
    private User user = new User();
    public static final Log LOG = LogFactory.getLog(DistinctUserMapper.class);
//    private  int year =
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
            String userName = valueSplits[0];
            String userID = valueSplits[1];
            String gender = valueSplits[2];
            String birthday = valueSplits[3];
            if (birthday == null || birthday == "" ||
                    birthday == " " || birthday.length() < 4)
                return;
            String subBirthday = birthday.substring(0, 4);
            int birthdayYear = 0;
            try
            {
                birthdayYear = Integer.parseInt(subBirthday);
            } catch (Exception ex)
            {
                LOG.error(ex);
            }
            Calendar now = Calendar.getInstance();
            int age = now.get(Calendar.YEAR) - birthdayYear;//简单计算一下年龄
            if (age <= 0 || age >= 100) return;
            user.setAge(age);
            user.setBirthday(birthday);
            user.setId(userID);
            user.setPersonName(userName);
            user.setGender(gender);
            outID.set(userID);
            context.write(outID, user);
        }
    }
}
