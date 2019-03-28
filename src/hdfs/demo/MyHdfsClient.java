package hdfs.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Jiaxi Wang on 2019/3/28 14:53
 */
public class MyHdfsClient {

    @Test
    /**
     * 上传文件
     */
    public void testUpload() throws Exception {
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS","hdfs://bigdata-01:9000/");
        conf.setInt("dfs.blocksize",134217728);
        conf.setInt("dfs.replication",3);

        //模拟root用户
        System.setProperty("HADOOP_USER_NAME","root");

        FileSystem fs = FileSystem.get(conf);

        fs.copyFromLocalFile(new Path("e:/TESTDATA/a.txt"),new Path("/"));

        fs.close();
    }
}
