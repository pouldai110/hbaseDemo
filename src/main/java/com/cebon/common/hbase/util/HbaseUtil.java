package com.cebon.common.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.util.StringUtils;

import java.util.Properties;

/**
 * @Auther: daiyp
 * @Date: 2019/6/13
 * @Description:
 */
public class HbaseUtil
{
    /**
     *
     * @param quorum zookeeper集群地址，以逗号分隔
     * @param port  zookeeper端口号
     * @param hadoopDir 客户端hadoop环境地址，可以为空
     * @param masterUrl hbasemaster 访问地址，可以为空
     * @return
     */
    public static  HbaseTemplate getHbaseTemplate(String quorum, String port, String hadoopDir, String masterUrl) {
        Properties properties = System.getProperties();
//        if(!StringUtils.isEmpty(hadoopDir)) {
//            properties.setProperty("hadoop.home.dir", hadoopDir);
//        }
        HbaseTemplate hbaseTemplate = new HbaseTemplate();

        Configuration conf = HBaseConfiguration.create();

        //zookeeper集群的URL配置信息
        conf.set("hbase.zookeeper.quorum", quorum);
        //客户端连接zookeeper端口
        conf.set("hbase.zookeeper.port", port);
        // hbase master
        if(!StringUtils.isEmpty(masterUrl)) {
          conf.set("hbase.master",masterUrl);
        }
        // hbase region

        //HBase RPC请求超时时间，默认60s(60000)
//        conf.setInt("hbase.rpc.timeout",20000);
//        //客户端重试最大次数，默认35
//        conf.setInt("hbase.client.retries.number",10);
//        //客户端发起一次操作数据请求直至得到响应之间的总超时时间，可能包含多个RPC请求，默认为2min
//        conf.setInt("hbase.client.operation.timeout",30000);
//        //客户端发起一次scan操作的rpc调用至得到响应之间的总超时时间
//        conf.setInt("hbase.client.scanner.timeout.period",200000);
        hbaseTemplate.setConfiguration(conf);
        hbaseTemplate.setAutoFlush(true);
        return hbaseTemplate;
    }
}
