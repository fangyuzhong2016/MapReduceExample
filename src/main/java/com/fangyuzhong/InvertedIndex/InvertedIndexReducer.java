package com.fangyuzhong.InvertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 定义输出Reduce函数
 *   输出为WordKey，keyword 为地址字符串分词后的关键字，后面 跟着 person信息和数量<p>
 *         如  安徽省:898776788(张三):2;876566556(李四):1<p>
 * Created by fangyuzhong on 17-6-26.
 */
public class InvertedIndexReducer extends Reducer<AddressKeyWord,Text,Text,Text>
{


    public void reduce(AddressKeyWord key, Iterable<Text> values, Context context)
            throws InterruptedException, IOException
    {
        Text result = new Text();

        String fileList = new String();
        for (Text value : values) {
            fileList += value.toString() + ";";
        }
        result.set(fileList);

        context.write(new Text(key.getKeyWord()), result);
    }
}
