package com.cebon.common.hbase.util.service;

import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

import java.util.List;
import java.util.Map;


public interface IHbaseTemplateService {
    /**
     * 在HBase上面创建表
     * @param tableName  表名
     * @param family 列族名(可以同时传入多个列族名)
     * @return
     */
    public boolean createTable(String tableName,String ...family);

    /**
     * Scan 查询所有的hbase数据
     * @param tableName 表名
     * @param <T> 返回数据类型
     * @return
     */
    public <T> List<T> searchAll(String tableName, Class<T> c);

    /**
     * 向表中插入数据
     * @param object
     * @param tableName
     * @param family
     * @param rowkey
     * @return
     */
    public Object createPro(Object object,String tableName,String family,String rowkey);

    /**
     * 通过表名和rowkey获取一行数据转object
     * @param <T> 数据类型
     * @param tableName 表名
     * @param rowkey
     * @return
     */
    public <T> T getOneToClass(Class<T> c,String tableName,String rowkey);

    /**
     * 根据表名组合查询
     * @param c
     * @param tableName
     * @param filterList 查询条件过滤器列表
     * @param <T>
     * @return
     */
    public <T>List<T> getListByCondition(Class<T> c, String tableName, FilterList filterList);

    /**
     * 通过表名和rowkey获取一行map数据
     * @param tableName
     * @param rowName
     * @return
     */
    public Map<String, Object> getOneToMap(String tableName, String rowName);

    /**
     * 查询一条记录一个column的值
     * @param tableName 表名
     * @param rowkey
     * @param family 列族
     * @param column 列
     * @return
     */
    public String getColumn(String tableName,String rowkey,String family,String column);

    /**
     * 查询开始row和结束row之间的数据
     * @param <T> 数据类型
     * @param tableName 表名
     * @param startRow 开始row
     * @param endRow 结束row
     * @return
     */
    public <T> List<T> findByRowRange(Class<T> c,String tableName,String startRow,String endRow);


    /**
     * *SingleColumnValueFilter scvf = new SingleColumnValueFilter(
     * Bytes.toBytes(family),  //搜索哪个列族
     * Bytes.toBytes(column),   //搜素哪一列
     * CompareFilter.CompareOp.EQUAL, //对比关系
     * Bytes.toBytes(Keywords)); //这里传入 SubstringComparator 比较器,搜索的结果为列值(value)包含关键字,传入bytes数组,则进行完全匹配
     * scvf.setLatestVersionOnly(true); //属性设置为true时,如果查询的列族下,没有colume这个列,则不返回这行数据,反之就返回这行数据
     * @param tableName
     * @param scvf
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> List<T> searchAllByFilter(Class<T> clazz, String tableName, SingleColumnValueFilter scvf);

}
