package com.cebon.common.hbase.util;

import jdk.nashorn.internal.objects.annotations.Setter;
import lombok.Data;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Auther: daiyp
 * @Date: 2019/6/13
 * @Description:
 */
@Slf4j
@Getter
public class HBaseObjectOpreat<T> {
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd hh:MM:ss");
    private Class<T> mappedClass;
    private Map<String, PropertyDescriptor> mappedFields;
    private Set<String> mappedProperties;
    private PropertyDescriptor[] pds;
    HashSet populatedProperties;
    private BeanWrapper beanWrapper;
    private T t;
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
                      if("java.util.Date".equals(c)){
                          map.put(key, this.simpleDateFormat.format(value));
                          continue;
                      }
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

    public T setProperetyToClass (String columnFamilyName, Result result)  {
        T t = null;
        this.mappedFields.forEach((key,pb)-> {
            String propertyName = pb.getName();
            byte[] resultValue = result.getValue(columnFamilyName.getBytes(), propertyName.getBytes());
            if(resultValue != null && resultValue.length>0) {
              Class c =  pb.getPropertyType();
              System.out.println(c.getName());
              setPropertyByType(c.getName(),t,pb.getWriteMethod(),new String(resultValue));
            }

        });
        return t;
    }
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


    private void setPropertyByType(String type, T t, Method method, String value) {

            try {

                if("java.lang.Byte".equals(type) || "byte".equals(type)) {
                    Byte byteValue = Byte.parseByte(value);
                    method.invoke(t,byteValue);
                }
                if("java.lang.Short".equals(type) || "short".equals(type)) {
                    Short shortvalue = Short.parseShort(value);
                    method.invoke(t,shortvalue);
                }
                if("java.lang.Integer".equals(type) || "int".equals(type)) {
                    Integer integer = Integer.parseInt(value);
                    method.invoke(t,value);
                }
                if("java.lang.Long".equals(type) || "long".equals(type)) {
                    Long longValue = Long.parseLong(value);
                    method.invoke(t,longValue);
                }
                if("java.lang.Float".equals(type) || "float".equals(type)) {
                    Float floatValue = Float.parseFloat(value);
                    method.invoke(t,floatValue);
                }
                if("java.lang.Double".equals(type) || "double".equals(type)) {
                    Double doublValue = Double.parseDouble(value);
                    method.invoke(t,doublValue);
                }
                if("java.lang.Boolean".equals(type) || "boolean".equals(type)) {
                    Boolean booleanValue = Boolean.parseBoolean(type);
                    method.invoke(t,booleanValue);
                }
                if("java.lang.String".equals(type)) {
                    method.invoke(t, value);
                }
                if("java.util.Date".equals(type)) {
                    Date date = this.simpleDateFormat.parse(value);
                    method.invoke(t,date);
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            }

    }

}
