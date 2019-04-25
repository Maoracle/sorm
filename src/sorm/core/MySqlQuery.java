package sorm.core;

import sorm.bean.ColumnInfo;
import sorm.bean.TableInfo;
import sorm.utils.JDBCUtils;
import sorm.utils.ReflectUtils;


import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 针对Mysql数据库的查询
 * @author Maoracle
 */

public class MySqlQuery implements Query {
    @Override
    public int executeDML(String sql, Object[] params) {
        Connection conn = DBManager.getConn();
        int count = 0;

        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            JDBCUtils.handleParams(ps, params);
            System.out.println(ps);
            count = ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBManager.close(ps, conn);
        }
        return count;
    }

    @Override
    public void insert(Object obj) {
        Class c= obj.getClass();
        List<Object> params = new ArrayList<>(); //存储sql的参数对象
        TableInfo tableInfo = TableContext.poClassTableMap.get(c);
        StringBuilder sql = new StringBuilder("insert into " + tableInfo.getTname() + " (");
        int countNotNullField = 0;
        Field[] fs = c.getDeclaredFields();
        for (Field f: fs) {
            String fieldName = f.getName();
            Object fieldValue = ReflectUtils.invokeGet(fieldName, obj);

            if (fieldValue != null) {
                countNotNullField++;
                sql.append(fieldName + "," );
                params.add(fieldValue);
            }
        }

        sql.setCharAt(sql.length() - 1, ')');
        sql.append(" values (");
        for (int i = 0; i < countNotNullField; i++) {
            sql.append("?,");
        }
        sql.setCharAt(sql.length() - 1, ')');
        executeDML(sql.toString(), params.toArray());
    }

    @Override
    public void delete(Class clazz, Object id) {
        //delete from clazz where id =
        //通过Class对象找TableInfo
        TableInfo tableInfo = TableContext.poClassTableMap.get(clazz);

        ColumnInfo onlyPriKey = tableInfo.getOnlyPriKey();

        String sql = "delete from " + tableInfo.getTname() + " where " + onlyPriKey.getName() + "=? ";

        executeDML(sql, new Object[]{id});
    }

    @Override
    public void delete(Object obj) {
        Class c = obj.getClass();
        TableInfo tableInfo = TableContext.poClassTableMap.get(c);
        ColumnInfo onlyPriKey = tableInfo.getOnlyPriKey();

        //通过反射机制，调用属性的对应的get方法或set方法
        Object priKeyValue = ReflectUtils.invokeGet(onlyPriKey.getName(), obj);
        delete(c, priKeyValue);
    }

    @Override
    public int update(Object obj, String[] fieldName) {
        Class c= obj.getClass();
        List<Object> params = new ArrayList<>(); //存储sql的参数对象
        TableInfo tableInfo = TableContext.poClassTableMap.get(c);
        ColumnInfo priKey = tableInfo.getOnlyPriKey(); //获得唯一主键
        StringBuilder sql = new StringBuilder("update " + tableInfo.getTname() + " set ");

        for (String fname: fieldName) {
            Object fvalue = ReflectUtils.invokeGet(fname, obj);
            params.add(fvalue);
            sql.append(fname+"=?,");
        }
        sql.setCharAt(sql.length() - 1, ' ');
        sql.append(" where ");
        sql.append(priKey.getName() + "=? ");

        params.add(ReflectUtils.invokeGet(priKey.getName(), obj));
        return executeDML(sql.toString(), params.toArray());
    }

    @Override
    public List queryRows(String sql, Class clazz, Object[] params) {
        Connection conn = DBManager.getConn();
        List list = null; //存储查询结果的容器
        PreparedStatement ps = null;
        ResultSet rs = null;

        ResultSetMetaData metaData = null;
        try {
            metaData = rs.getMetaData();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            ps = conn.prepareStatement(sql);
            JDBCUtils.handleParams(ps, params);
            System.out.println(ps);
            rs = ps.executeQuery();
            while (rs.next()) {
                if (list == null) {
                    list = new ArrayList();
                }
                Object rowObj = clazz.newInstance(); //调用javabean的无参构造器

                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnLabel(i + 1);
                    Object columnValue = rs.getObject(i + 1);

                    //调用rowObj对象的setUsername方法，将columnValue的值设进去
                    ReflectUtils.invokeSet(rowObj, columnName, columnValue);
                }
                list.add(rowObj);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBManager.close(ps, conn);
        }
        return list;
    }

    @Override
    public Object queryUniqueRows(String sql, Class clazz, Object[] params) {
        List list = queryRows(sql, clazz, params);
        return (list == null && list.size() > 0) ? null : list.get(0);
    }

    @Override
    public Object queryValue(String sql, Object[] params) {
        Connection conn = DBManager.getConn();
        Object value = null; //存储查询结果的对象
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(sql);
            JDBCUtils.handleParams(ps, params);
            System.out.println(ps);
            rs = ps.executeQuery();
            while (rs.next()) {
                value = rs.getObject(1);
            }
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBManager.close(ps, conn);
        }
        return value;
    }

    @Override
    public Number queryNumber(String sql, Object[] params) {
        return (Number)queryValue(sql, params);
    }
}
