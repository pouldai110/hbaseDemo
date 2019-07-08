package com.cebon.common.hbase.util.service;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Auther: daiyp
 * @Date: 2019/6/19
 * @Description: hdfs文件操作
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class HDFSTemplateService {

    private String path;

    private String username;

    private static String hdfsPath = "hdfs://192.168.99.246:9000";
    private static String hdfsName = "root";

    /**
     * 获取HDFS配置信息
     *
     * @return
     */
    private static Configuration getConfiguration() {

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsPath);
        return configuration;
    }

    /**
     * 获取HDFS文件系统对象
     *
     * @return
     * @throws Exception
     */
    public static FileSystem getFileSystem() throws Exception {
        // 客户端去操作hdfs时是有一个用户身份的，默认情况下hdfs客户端api会从jvm中获取一个参数作为自己的用户身份 DHADOOP_USER_NAME=hadoop
//        FileSystem hdfs = FileSystem.get(getHdfsConfig()); //默认获取
//        也可以在构造客户端fs对象时，通过参数传递进去
        FileSystem fileSystem = FileSystem.get(new URI(hdfsPath), getConfiguration(), hdfsName);
        return fileSystem;
    }

    /**
     * 新建文件夹
     *
     * @param path 目的地址
     * @return
     * @throws Exception
     */
    public String mkdir(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return "请求参数为空";
        }
        // 文件对象
        FileSystem fs = this.getFileSystem();
       try{
        // 目标路径
        Path newPath = new Path(path);
        // 创建空文件夹
        boolean isOk = fs.mkdirs(newPath);

        if (isOk) {
            return "create dir success";
        } else {
            return "create dir fail";
        }}finally {
           fs.close();
       }
    }

    /**
     * 上传指定文件
     * @param path 文件地址
     * @param file 文件信息
     * @return
     * @throws Exception
     */
    public String createFile(String path, File file) throws Exception {
        if (StringUtils.isEmpty(path) || null == file) {
            return "请求参数为空";
        }
        String fileName = file.getName();
        FileSystem fs = null;
        FSDataOutputStream outputStream = null;
        InputStream in = null;
        try {
            fs = this.getFileSystem();
            // 上传时默认当前目录，后面自动拼接文件的目录
            Path newPath = new Path(path + "/" + fileName);
            // 打开一个输出流
            outputStream = fs.create(newPath);

            in = new FileInputStream(file);
            byte[] bytes = new byte[4096];
            int tempByte;
            while ((tempByte = in.read(bytes)) != -1) {
                outputStream.write(bytes, 0, tempByte);
            }
        }  finally {
                in.close();
                outputStream.close();
                fs.close();

        }

        return "create file success";
    }

    /**
     * 读取指定文件
     * @param path 文件地址
     * @param outputStream 输出流
     * @throws Exception
     */
    public void readFile(String path, OutputStream outputStream) throws Exception {
        FileSystem fs = this.getFileSystem();
        Path newPath = new Path(path);
        InputStream in = null;
        try {
            in = fs.open(newPath);
            // 复制到标准的输出流
            IOUtils.copyBytes(in, outputStream, 4096);
        } finally {
            IOUtils.closeStream(in);
            fs.close();
        }

    }

    /**
     *读取指定地址目录所有信息
     * @param path 目录地址
     * @return
     * @throws Exception
     */
    public List readPathInfo(String path) throws Exception {
        FileSystem fs = this.getFileSystem();
        Path newPath = new Path(path);
        FileStatus[] statusList = fs.listStatus(newPath);
        List<Map<String, Object>> list = new ArrayList<>();
        if (null != statusList && statusList.length > 0) {
            for (FileStatus fileStatus : statusList) {
                Map<String, Object> map = new HashMap<>();
                map.put("filePath", fileStatus.getPath());
                map.put("fileStatus", fileStatus.toString());
                list.add(map);
            }
            return list;
        } else {
            return null;
        }
    }

    /**
     * 读取指定地址下所有文件信息
     * @param path 地址
     * @return
     * @throws Exception
     */
    public List listFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        FileSystem fs = this.getFileSystem();
       try {
        Path newPath = new Path(path);
        // 递归找到所有文件
        RemoteIterator<LocatedFileStatus> filesList = fs.listFiles(newPath, true);
        List<Map<String, String>> returnList = new ArrayList<>();
        while (filesList.hasNext()) {
            LocatedFileStatus next = filesList.next();
            String fileName = next.getPath().getName();
            Path filePath = next.getPath();
            Map<String, String> map = new HashMap<>();
            map.put("fileName", fileName);
            map.put("filePath", filePath.toString());
            returnList.add(map);

        }
           return returnList;
       }finally {
           fs.close();
       }
    }

    public String  deleteFile( String path) throws Exception {
        FileSystem fs = this.getFileSystem();
        Path newPath = new Path(path);
        boolean isOk = fs.deleteOnExit(newPath);
        fs.close();
        if (isOk) {
            return "delete file success";
        } else {
            return "delete file fail";
        }
    }

    /**
     * 上传指定文件信息
     * @param path 上传地址
     * @param uploadPath 文件地址
     * @return
     * @throws Exception
     */
    public String uploadFile(String path,String uploadPath) throws Exception {
        FileSystem fs = this.getFileSystem();
       try {
           // 上传路径
           Path clientPath = new Path(path);
           // 目标路径
           Path serverPath = new Path(uploadPath);

           // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
           fs.copyFromLocalFile(false, clientPath, serverPath);
           return "upload file success";
       } finally {
           fs.close();
       }

    }

    /**
     * 下载指定文件信息
     * @param path 文件地址
     * @param downloadPath 下载地址
     * @return
     * @throws Exception
     */
    public String downloadFile(String path, String downloadPath) throws Exception {
        FileSystem fs = this.getFileSystem();
        // 上传路径
        Path clientPath = new Path(path);
        // 目标路径
        Path serverPath = new Path(downloadPath);

        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
        fs.copyToLocalFile(false, clientPath, serverPath);
        fs.close();
        return "download file success";
    }

    /**
     * 复制文件到指定地址
     * @param sourcePath 需要复制到文件地址
     * @param targetPath 复制文件目标地址
     * @return
     * @throws Exception
     */
    public String copyFile(String sourcePath,String targetPath) throws Exception {
        FileSystem fs = this.getFileSystem();
        // 原始文件路径
        Path oldPath = new Path(sourcePath);
        // 目标路径
        Path newPath = new Path(targetPath);

        FSDataInputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        try {
            inputStream = fs.open(oldPath);
            outputStream = fs.create(newPath);
            IOUtils.copyBytes(inputStream, outputStream, 1024*1024*64,false);
            return "copy file success";
        } finally {
            inputStream.close();
            outputStream.close();
            fs.close();
        }
    }


}
