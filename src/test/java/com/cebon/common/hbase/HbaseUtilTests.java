package com.cebon.common.hbase;

import com.cebon.common.hbase.util.HBaseObjectOpreat;
import com.cebon.common.hbase.util.HbaseUtil;
import com.cebon.common.hbase.util.User;
import com.cebon.common.hbase.util.service.HbaseTemplateService;
import com.cebon.common.hbase.util.service.IHbaseTemplateService;
import org.junit.Test;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @Auther: daiyp
 * @Date: 2019/6/13
 * @Description:
 */
public class HbaseUtilTests
{
    @Test
    public  void testGetTemplet(){
        String ip  = "192.168.99.246";
        String port = "2181";
        String hadoopdir = "D:\\app\\hadoop-common-2.6.0-bin-master";
        String masterUrl = "192.168.99.246:16010";
        HbaseTemplate hbaseTemplate = HbaseUtil.getHbaseTemplate(ip,port,hadoopdir,masterUrl);
        IHbaseTemplateService hbaseTemplateService = new HbaseTemplateService(hbaseTemplate);
        User user  = new  User();
        user.setName("111");
        user.setPassowd("222");
        user.setSex(3);
        user.setDate(new Date());
        // hbaseTemplate.put("user","rk0005","info","name",);
        hbaseTemplateService.createPro(user,"user","info","test1");
        User user1 = hbaseTemplateService.getOneToClass(User.class,"user","test1");

        System.out.println("info1111111111:"+user1.toString());
    }
//    @Test
//    public  void testOpreat() {
//        User user  = new  User();
//        user.setName("111");
//        user.setPassowd("222");
//        user.setSex(3);
//        HBaseObjectOpreat<User> baseObjectOpreat = new HBaseObjectOpreat<>(User.class);
//        Map map = baseObjectOpreat.getClassProperty(user);
//        if(map !=null) {
//            map.forEach((k, value) -> System.out.println(k + "" + value));
//        }
//    }
}
