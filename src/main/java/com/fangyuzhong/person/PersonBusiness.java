package com.fangyuzhong.person;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * MapReduce 业务类
 * Created by fangyuzhong on 17-6-2.
 */
public class PersonBusiness implements Writable
{
    private String id = "";
    private String personName = "";
    private long personCount = 0;
    private Date personVistTimes = new Date();
    private String visthotlTimes = "";

    /**
     * 获取用户的身份证ID
     *
     * @return
     */
    public String getId()
    {
        return id;
    }

    /**
     * @param id
     */
    public void setId(String id)
    {
        this.id = id;
    }

    /**
     * @return
     */
    public String getPersonName()
    {
        return personName;
    }

    /**
     * @param name
     */
    public void setPersonName(String name)
    {
        this.personName = name;
    }

    /**
     * @return
     */
    public long getPersonCount()
    {
        return personCount;
    }

    /**
     * @param count
     */
    public void setPersonCount(long count)
    {
        this.personCount = count;
    }

    /**
     * @return
     */
    public Date getPersonVistTimes()
    {
        return personVistTimes;
    }

    /**
     * @param vistTimes
     */
    public void setPersonVistTimes(Date vistTimes)
    {
        this.personVistTimes = vistTimes;
    }

    /**
     * @return
     */
    public String getVisthotlTimes()
    {
        return visthotlTimes;
    }

    /**
     * @param visthotlTimes
     */
    public void setVisthotlTimes(String visthotlTimes)
    {
        this.visthotlTimes = visthotlTimes;
    }

    /**
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException
    {

        id = in.readUTF();
        personName = in.readUTF();
        personCount = in.readLong();
        personVistTimes = new Date(in.readLong());
        visthotlTimes = in.readUTF();
    }

    /**
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException
    {

        out.writeUTF(id);
        out.writeUTF(personName);
        out.writeLong(personCount);
        out.writeLong(personVistTimes.getTime());
        out.writeUTF(visthotlTimes);

    }

    public String toString()
    {
        return  personName+"-到店时间段["+visthotlTimes+"] 总共住过"+ personCount+"次"+"\t";
    }

}
