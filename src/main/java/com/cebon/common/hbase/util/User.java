package com.cebon.common.hbase.util;

import lombok.Data;

import java.util.Date;

/**
 * @Auther: daiyp
 * @Date: 2019/6/13
 * @Description:
 */
@Data
public class User {
    private String name;
    private String passowd;
    private int sex;
    private String age;
    private Date date;

}
