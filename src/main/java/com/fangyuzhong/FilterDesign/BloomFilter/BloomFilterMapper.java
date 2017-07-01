package com.fangyuzhong.FilterDesign.BloomFilter;

import com.fangyuzhong.InvertedIndex.AddressKeyWord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;


/**
 * 给定用户住址的相关关键字列表和用户信息，过滤出用户地址在列表中的用户的相关信息。</p>
 *   如：给定的列表 </p>
 ******************</p>
 *  <p> 安徽</p>
 *  <p> 安庆</p>
 *  <p> 北京</p>
 *  <p> 天津</p>
 *  <p> 杭州</p>
 ******************
 *   <p>用户列表
 *****************************
 * <p>张三,XXXXXXXXX,M,19840980,XXXXXXXXXXXXXXXXXXXX
 *<p> 李四,XXXXXXXXX,M,19840980,XXXXXXXXXXXXXXXXXXXX
 * <p>王五,XXXXXXXXX,M,19840980,XXXXXXXXXXXXXXXXXXXX</p>
 **************************************************
 *取出用户地址信息然后分词，每个判断是否在地址列表中，在然后输出</p>
 *
 * Created by fangyuzhong on 17-6-29.
 */
public class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable>
{

    //HDFS 的 BloomFilter 对象
    private BloomFilter filter = new BloomFilter();

    /**
     * 从分布式缓存中获取热点列表的二进制文件缓存到本地
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        super.setup(context);
       Path[] files =context.getLocalCacheFiles();//目前还是使用这个方法获取缓存吧，虽然过时了。没找到相关方法
        DataInputStream dataInputStream = new DataInputStream(new FileInputStream(files[0].toString()));
        filter.readFields(dataInputStream);
        dataInputStream.close();
    }

    /**
     * map函数
     * 解析房客地址信息，然后通过中文分词方法对地址进行分词，对每个分词，
     * 判断是否在布隆过滤器中，是，则使用map输出，输出Mapper输入的value<p>
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
        if(valueSplits.length>=7)
        {
            String userName = valueSplits[0];
            String address = valueSplits[4];//酒店房客地址
            if (address != "")
            {
                //创建分词对象，解析出来的地址进行分词
                Analyzer anal = new IKAnalyzer(true);
                StringReader reader = new StringReader(address);
                //地址分词
                TokenStream tokenStream = anal.tokenStream("", reader);
                CharTermAttribute charTermAttribute = tokenStream.getAttribute(CharTermAttribute.class);
                //遍历分词数据
                tokenStream.reset();
                while (tokenStream.incrementToken())
                {
                    String keyword = charTermAttribute.toString();
                    if(keyword==null||keyword=="") continue;
                    //判断地址分词的单个词是否在布隆过滤器中，将查询出来的数据进行输出
                    if(filter.membershipTest(new Key(keyword.getBytes("UTF-8"))))
                    {
                        context.write(value,NullWritable.get());
                        break;
                    }
                }
                reader.close();
            }
        }
    }
}
