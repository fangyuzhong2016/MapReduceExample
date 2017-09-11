package com.fangyuzhong.FilterDesign.TopFilterN;

import com.fangyuzhong.InvertedIndex.AddressKeyWord;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

/**
 * 用户对象类
 * Created by fangyuzhong on 17-7-4.
 */
public class User implements WritableComparable<User>
{

    private String id = "";
    private String personName = "";
    private String birthday="";
    private String gender="";
    private int age=0;


    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public String getPersonName()
    {
        return personName;
    }

    public void setPersonName(String personName)
    {
        this.personName = personName;
    }

    public String getBirthday()
    {
        return birthday;
    }

    public void setBirthday(String birthday)
    {
        this.birthday = birthday;
    }

    public String getGender()
    {
        return gender;
    }

    public void setGender(String gender)
    {
        this.gender = gender;
    }

    public int getAge()
    {
        return age;
    }

    public void setAge(int age)
    {
        this.age = age;
    }

    /**
     * 反序列化对象
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException
    {
        id = in.readUTF();
        personName = in.readUTF();
        birthday = in.readUTF();
        gender = in.readUTF();
        age = in.readInt();
    }

    /**
     * 序列化对象
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(id);
        out.writeUTF(personName);
        out.writeUTF(birthday);
        out.writeUTF(gender);
        out.writeInt(age);
    }

    @Override
    public String toString()
    {
        return id+","+personName+","+gender+","+birthday+","+age;
    }

    public boolean equals(Object obj)
    {
        if(obj instanceof User)
        {
            User user = (User)obj;
            return id.equals(user.id);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public int compareTo(User o)
    {
        if (id != o.getId())
            return id.compareTo(o.id);
        return personName.compareTo(o.personName);
    }
}
