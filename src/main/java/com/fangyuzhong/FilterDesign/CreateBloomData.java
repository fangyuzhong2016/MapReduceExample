package com.fangyuzhong.FilterDesign;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.jets3t.service.io.GZipDeflatingInputStream;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * 生成布隆过滤器的二进制数据
 * Created by fangyuzhong on 17-6-29.
 */
public class CreateBloomData
{

    /**
     * 计算二进制数组大小
     * @param numRecords 记录字符串长度
     * @param falsePosRate 误报率
     * @return
     */
    public static int getOptimalBloomFilterSize(int numRecords,float falsePosRate)
    {
        int size = (int)(-numRecords*(float)Math.log(falsePosRate)/Math.pow(Math.log(2),2));
        return size;
    }

    /**
     * 获取哈希函数的个数
     * @param numMembers
     * @param vectorSize
     * @return
     */
    public static int getOptimalk(float numMembers,float vectorSize)
    {
        return (int)Math.round(vectorSize/numMembers*Math.log(2));
    }


    /**
     * 借助Hadoop布隆过滤器的实现，创建布隆过滤器二进制文件
     * @param filePath  输入的文件路径
     * @param numMembers 创建布隆过滤器数组的长度
     * @param falsePosRate 错误率
     * @param bfFilePath 创建布隆过滤器文件的输出路径
     */
    public static void CreateBloom(String filePath,int numMembers,float falsePosRate,String bfFilePath) throws Exception
    {

        Path inputFile = new Path(filePath);
        Path bfFle = new Path(bfFilePath);
        FileSystem fileSystem=FileSystem.get(new Configuration());
        if(fileSystem.exists(bfFle))
        {
            fileSystem.delete(bfFle, true);
            System.out.println("存在此路径，已经删除！");
        }
        int vectorSize = getOptimalBloomFilterSize(numMembers,falsePosRate);
        int nbHash = getOptimalk(numMembers,vectorSize);

        //创建一个新的布隆过滤器
        BloomFilter filter = new BloomFilter(vectorSize,nbHash, Hash.MURMUR_HASH);

        String line = null;
        int numRecords =0;
        FileSystem fs = FileSystem.get(new Configuration());
        for(FileStatus fileStatus:fs.listStatus(inputFile))
        {
            BufferedReader bufferedReader;
            if(fileStatus.getPath().getName().endsWith(".gz"))
            {
                bufferedReader = new BufferedReader(new InputStreamReader(
                        new GZipDeflatingInputStream(fs.open(fileStatus.getPath()))));
            }
            else
            {
                bufferedReader = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
            }
            while ((line = bufferedReader.readLine()) != null)
            {
                filter.add(new Key(line.getBytes()));
                ++numRecords;
            }
            bufferedReader.close();
        }
        FSDataOutputStream fsDataOutputStream = fs.create(bfFle);
        filter.write(fsDataOutputStream);
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }

    public static void main(String[] args) throws Exception
    {
        String inputFile = "hdfs://192.168.2.2:8020/input/";
        String bfFile = "hdfs://192.168.2.2:8020/input/bloom.bin";
        CreateBloom(inputFile,10,0.01f,bfFile);
    }
}
