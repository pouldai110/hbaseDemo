package com.cebon.common.hbase.util.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HbaseTemplateService  implements IHbaseTemplateService{
    private HbaseTemplate hbaseTemplate;

    public HbaseTemplateService(HbaseTemplate hbaseTemplate) {
        this.hbaseTemplate = hbaseTemplate;
    }

    @SuppressWarnings({"resource", "deprecation"})
    @Override
    public boolean createTable(String tableName, String... column) {
        HBaseAdmin admin;
        try {
            // 从hbaseTemplate 获取configuration对象,用来初始化admin
            admin = new HBaseAdmin(hbaseTemplate.getConfiguration());
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            for (int i = 0; i < column.length; i++) {
                tableDescriptor.addFamily(new HColumnDescriptor(column[i]));
            }
            admin.createTable(tableDescriptor);
            return admin.tableExists(tableName);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private  <T>List<T> getListByCondition(Class<T> c, String tableName, Scan scan){
        if (c == null || StringUtils.isBlank(tableName)) {
            return null;
        }
        return hbaseTemplate.find(tableName, scan, new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int rowNum) throws Exception {
                T pojo = c.newInstance();
                BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(pojo);
                List<Cell> ceList = result.listCells();
                for (Cell cellItem : ceList) {
                    String cellName = new String(CellUtil.cloneQualifier(cellItem));
                    if (!"class".equals(cellName)) {
                        beanWrapper.setPropertyValue(cellName, new String(CellUtil.cloneValue(cellItem)));
                    }
                }
                return pojo;
            }
        });
    }

    @Override
    public <T> List<T> searchAll(String tableName, Class<T> c) {
        return this.getListByCondition(c,tableName,new Scan());
    }

    @Override
    public Object createPro(Object pojo, String tableName, String column, String rowkey) {
        if (pojo == null || StringUtils.isBlank(tableName) || StringUtils.isBlank(column)) {
            return null;
        }
        return hbaseTemplate.execute(tableName, new TableCallback<Object>() {
            @Override
            public Object doInTable(HTableInterface table) throws Throwable {
                PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(pojo.getClass());
                BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(pojo);
                Put put = new Put(Bytes.toBytes(rowkey));
                for (PropertyDescriptor propertyDescriptor : pds) {
                    String properName = propertyDescriptor.getName();
                    String properType = propertyDescriptor.getPropertyType().getCanonicalName();
                    String value = beanWrapper.getPropertyValue(properName)== null ? null:beanWrapper.getPropertyValue(properName).toString();
                    if(StringUtils.isNotEmpty(value) && "java.util.Date".equals(properType)) {
                        Date date = (Date)beanWrapper.getPropertyValue(properName);
                        value = date.getTime()+"";
                    }
                    if (!StringUtils.isBlank(value)) {
                        put.add(Bytes.toBytes(column), Bytes.toBytes(properName), Bytes.toBytes(value));
                    }
                }
                table.put(put);
                return null;
            }
        });
    }

    @Override
    public <T> T getOneToClass(Class<T> c, String tableName, String rowkey) {
        if (c == null || StringUtils.isBlank(tableName) || StringUtils.isBlank(rowkey)) {
            return null;
        }
        return hbaseTemplate.get(tableName, rowkey, new RowMapper<T>() {
            public T mapRow(Result result, int rowNum) throws Exception {
                List<Cell> ceList = result.listCells();
                JSONObject obj = new JSONObject();
                T item = c.newInstance();
                if (ceList != null && ceList.size() > 0) {
                    for (Cell cell : ceList) {
//                        cell.getTimestamp();
//                        //rowKey
//                        CellUtil.cloneRow(cell);
                        obj.put(
                                Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                                        cell.getQualifierLength()),
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    }
                } else {
                    return null;
                }
                item = JSON.parseObject(obj.toJSONString(), c);
                return item;
            }
        });
    }

    @Override
    public <T>List<T> getListByCondition(Class<T> c, String tableName, FilterList filterList){
        if (c == null || StringUtils.isBlank(tableName)) {
            return null;
        }
        Scan scan=new Scan();
        scan.setFilter(filterList);
        return this.getListByCondition(c,tableName,scan);
    }

    @Override
    public Map<String, Object> getOneToMap(String tableName, String rowName) {
        return hbaseTemplate.get(tableName, rowName,new RowMapper<Map<String,Object>>(){
            @Override
            public Map<String, Object> mapRow(Result result, int i) throws Exception {
                List<Cell> ceList =   result.listCells();
                Map<String,Object> map = new HashMap<String, Object>();
                if(ceList!=null&&ceList.size()>0){
                    for(Cell cell:ceList){
                        map.put(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+
                                        "_"+Bytes.toString( cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()),
                                Bytes.toString( cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    }
                }
                return map;
            }
        });
    }

    @Override
    public String getColumn(String tableName, String rowkey, String family, String column) {
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(family)
                || StringUtils.isBlank(rowkey) || StringUtils.isBlank(column)) {
            return null;
        }
        return hbaseTemplate.get(tableName, rowkey, family, column, new RowMapper<String>() {
            public String mapRow(Result result, int rowNum) throws Exception {
                List<Cell> ceList = result.listCells();
                String res = "";
                if (ceList != null && ceList.size() > 0) {
                    for (Cell cell : ceList) {
                        res =
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    }
                }
                return res;
            }
        });
    }

    @Override
    public <T> List<T> findByRowRange(Class<T> c, String tableName, String startRow, String endRow) {
        if (c == null || StringUtils.isBlank(tableName) || StringUtils.isBlank(startRow)
                || StringUtils.isBlank(endRow)) {
            return null;
        }
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));
        scan.setCacheBlocks(false);
        scan.setCaching(2000);
        return this.getListByCondition(c,tableName,scan);
    }

    @Override
    public <T> List<T> searchAllByFilter(Class<T> clazz,String tableName,SingleColumnValueFilter scvf) {
        Scan scan = new Scan();
        scan.setFilter(scvf);
        return this.getListByCondition(clazz,tableName,scan);
    }
}
