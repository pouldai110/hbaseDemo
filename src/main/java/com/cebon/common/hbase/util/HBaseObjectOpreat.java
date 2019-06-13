package com.cebon.common.hbase.util;

import jdk.nashorn.internal.objects.annotations.Setter;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.util.StringUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * @Auther: daiyp
 * @Date: 2019/6/13
 * @Description:
 */
@Slf4j
@Getter
public class HBaseObjectOpreat<T> {
    private Class<T> mappedClass;
    private Map<String, PropertyDescriptor> mappedFields;
    private Set<String> mappedProperties;
    private PropertyDescriptor[] pds;
    HashSet populatedProperties;
    private BeanWrapper beanWrapper;
    public HBaseObjectOpreat(Class<T> clazz) {
        this.mappedClass = clazz;
        mappedFields = new HashMap<>();
        mappedProperties = new HashSet<>();
        populatedProperties = new HashSet<>();
        this.pds = BeanUtils.getPropertyDescriptors(mappedClass);
        for (PropertyDescriptor pd :pds ) {
            if (pd.getWriteMethod() != null) {
                this.mappedFields.put(this.lowerCaseName(pd.getName()), pd);
                String underscoredName = this.underscoreName(pd.getName());
                if (!this.lowerCaseName(pd.getName()).equals(underscoredName)) {
                    this.mappedFields.put(underscoredName, pd);
                }
                this.mappedProperties.add(pd.getName());
            }
        }

    }

    public  Map getClassProperty(T t) {
        Map map = new HashMap();
        for (PropertyDescriptor pd :pds ) {
            if (pd.getReadMethod() != null) {
                String key = pd.getName();
                Method method = pd.getReadMethod();
                try {
                  Object value = method.invoke(t);
                   Class c = pd.getPropertyType();
                    System.out.println(c.getName());
//                  System.out.println(key+":"+value);
                  if(value != null) {
                      map.put(key, value);
                  }
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

//    public T setProperetyToClass (String columnFamilyName, Result result)  {
//        T t;
//        this.mappedFields.forEach((key,value)-> {
//            String propertyName = value.getName();
//            byte[] resultValue = result.getValue(columnFamilyName.getBytes(), propertyName.getBytes());
//            if(resultValue != null && resultValue.length>0) {
//
//              Class c =  value.getPropertyType();
//              System.out.println(c.getName());
//            }
//
//        });
//        return t;
//    }
    private String underscoreName(String name) {
        if (!StringUtils.hasLength(name)) {
            return "";
        } else {
            StringBuilder result = new StringBuilder();
            result.append(this.lowerCaseName(name.substring(0, 1)));

            for (int i = 1; i < name.length(); ++i) {
                String s = name.substring(i, i + 1);
                String slc = this.lowerCaseName(s);
                if (!s.equals(slc)) {
                    result.append("_").append(slc);
                } else {
                    result.append(s);
                }
            }

            return result.toString();
        }
    }

    private String lowerCaseName(String name) {
        return name.toLowerCase(Locale.US);
    }

    //使用时根据要解析的字段频繁调用此方法即可，仿造java8 流式操作
    public HBaseObjectOpreat build(String columnName, String columnFamilyName, Result result) {
        byte[] value = result.getValue(columnFamilyName.getBytes(), columnName.getBytes());
        if (value == null || value.length == 0) {
            return this;
        } else {
            String field = this.lowerCaseName(columnName.replaceAll(" ", ""));
            PropertyDescriptor pd = this.mappedFields.get(field);
            if (pd == null) {
                log.error("HBaseObjectOpreat error: can not find property: " + field);
            } else {
                beanWrapper.setPropertyValue(pd.getName(), Bytes.toString(value));
                populatedProperties.add(pd.getName());
            }
        }
        return this;
    }

}
