package com.fangyuzhong.InvertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

/**
 * 定义倒排索引的Map类<p>
 *     Map类输入的key value 是Object类型和Text类型，输出为AddressKeyWord类型和Text类型<p>
 *         Map函数中先将酒店房客的籍贯地址解析出来，然后借助IKAnalyzer中文分词将地址解析为keyword
 *<p>
 * Created by fangyuzhong on 17-6-26.
 */
public class InvertedIndexMapper extends Mapper<Object,Text,AddressKeyWord,Text>
{

    private Text outCount = new Text();


    /**
     *Map函数中先将酒店房客的籍贯地址解析出来，然后借助IKAnalyzer中文分词将地址解析为keyword
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key,Text value,Context context)
        throws IOException,InterruptedException
    {
        String[] valueSplits = value.toString().split(",");
        if(valueSplits.length>=7)
        {
            String personName = valueSplits[0];/*酒店房客姓名*/
            String personId = valueSplits[1];//酒店房客ID
            String address = valueSplits[4];//酒店房客地址
            StringBuffer description = new StringBuffer(personId).append("(").append(personName).append(")");
            if (address != "")
            {
                //创建分词对象，解析出来的地址进行分词
                Analyzer anal = new IKAnalyzer(true);
                StringReader reader = new StringReader(address);
                //地址分词
                TokenStream tokenStream = anal.tokenStream("", reader);
                CharTermAttribute charTermAttribute = tokenStream.getAttribute(CharTermAttribute.class);
                //遍历分词数据
                tokenStream.reset();//解决TokenStream contract violation: reset()/close() call missing,异常
                while (tokenStream.incrementToken())
                {
                    String keyword = charTermAttribute.toString();
                    if(keyword==null||keyword=="") continue;
                    outCount.set("1");
                    AddressKeyWord addressKeyWord = new AddressKeyWord();
                    addressKeyWord.setKeyWord(keyword);
                    addressKeyWord.setDescription(description.toString());
                    context.write(addressKeyWord, outCount);
                }
                reader.close();

            }
        }
    }
}
