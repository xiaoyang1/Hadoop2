package atguigu.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class TestCompress {
    public static void main(String[] args) throws Exception{
//        compress("./hello.txt","org.apache.hadoop.io.compress.BZip2Codec");
        decompress("./hello.txt.bz2");
    }

    private static void compress(String filename, String method) throws Exception{
        // 获得输入流
        FileInputStream fis = new FileInputStream(new File(filename));

        Class codecClass = Class.forName(method);

        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());

        // 2 获取输出流
        FileOutputStream fos = new FileOutputStream(new File(filename + codec.getDefaultExtension()));

        CompressionOutputStream cos = codec.createOutputStream(fos);

        IOUtils.copyBytes(fis, cos, 1024*1024*5, false);

        // 4 关闭资源
        fis.close();
        cos.close();
        fos.close();
    }

    private static void decompress(String filename) throws FileNotFoundException, IOException {

        // 0 校验是否能解压缩
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));

        if(codec == null){
            System.out.println("cannot find codec for file " + filename);
            return;
        }


        FileInputStream fis = new FileInputStream(filename);
        CompressionInputStream cis = codec.createInputStream(fis);

        // 2 获取输出流
        FileOutputStream fos = new FileOutputStream(new File(filename + ".decoded"));

        IOUtils.copyBytes(cis, fos, 1024*1024*5, false);

        fos.close();
        cis.close();
        fis.close();
    }
}
