package com.fangyuzhong.InvertedIndex;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 定义倒排索引Map输出的类型<p>
 *     keyword 为地址字符串分词后的关键字，后面 跟着 person信息和数量<p>
 *         如  安徽省:898776788(张三):2;876566556(李四):1<p>
 * Created by fangyuzhong on 17-6-26.
 */
public class AddressKeyWord implements WritableComparable<AddressKeyWord>
{

    private String keyWord="";
    private String description="";


    /**
     * 设置倒排索引关键字
     * @param keyWord
     */
    public void setKeyWord(String keyWord)
    {
        this.keyWord = keyWord;
    }

    /**
     * 获取倒排索引关键字
     * @return
     */
    public String getKeyWord()
    {
        return keyWord;
    }

    /**
     * 设置倒排索引的内容
     * @param description
     */
    public void setDescription(String description)
    {
        this.description=description;
    }

    /**
     * 获取倒排索引的内容
     * @return
     */
    public String getDescription()
    {
        return description;
    }


    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException
    {
        keyWord = in.readUTF();

        description=in.readUTF();
    }

    /**
     * 序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(keyWord);

        out.writeUTF(description);
    }

    /**
     * 自定义倒排索引对象比较器
     * @param arg0
     * @return
     */
    @Override
    public int compareTo(AddressKeyWord arg0)
    {
        if (keyWord != arg0.keyWord)
            return keyWord.compareTo(arg0.keyWord);
        return description.compareTo(arg0.description);
    }
}
