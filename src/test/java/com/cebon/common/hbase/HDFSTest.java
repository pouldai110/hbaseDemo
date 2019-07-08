package com.cebon.common.hbase;

import com.cebon.common.hbase.util.service.HDFSTemplateService;
import org.junit.Test;

import java.io.*;

/**
 * @Auther: daiyp
 * @Date: 2019/7/8
 * @Description:
 */
public class HDFSTest {
    HDFSTemplateService hdfsTemplateService = new HDFSTemplateService();
    @Test
    public void  testMkdir() {
        try {
            System.out.println( hdfsTemplateService.mkdir("test"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public  void testUploadFile() {
        File file = new File("d:/fftest.txt");
        try {
            hdfsTemplateService.createFile("test",file);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public  void downLoad() {
        File file = new File("d:/fftest1113.txt");
        ByteArrayOutputStream outputStream = null;
        FileInputStream inputStream;
        try {
            outputStream =new ByteArrayOutputStream();
            hdfsTemplateService.readFile("test/fftest.txt",outputStream);
            FileOutputStream outputStream1 = new FileOutputStream(file);
            outputStream1.write(outputStream.toByteArray());
            outputStream1.close();
//            inputStream = new FileInputStream(file);
//            inputStream.read(outputStream.toByteArray());
//            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
