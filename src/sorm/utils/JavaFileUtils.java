package sorm.utils;

import sorm.bean.ColumnInfo;
import sorm.bean.JavaFieldGetSet;
import sorm.bean.TableInfo;
import sorm.core.DBManager;
import sorm.core.MySqlTypeConvertor;
import sorm.core.TableContext;
import sorm.core.TypeConvertor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 封装了生成JAVA文件（源代码）常用的操作
 * @author Maoracle
 */
public class JavaFileUtils {

    /**
     * 根据字段信息生成java属性信息。如：varchar username-->private String username;以及相应的set和get方法源码
     * @param column 字段信息
     * @param convertor 类型转换器
     * @return java属性和set/get源码
     */
    public static JavaFieldGetSet createFieldGetSetSRC(ColumnInfo column, TypeConvertor convertor) {
        JavaFieldGetSet javaFieldGetSet = new JavaFieldGetSet();

        String javaFieldType = convertor.databaseType2JavaType(column.getDataType());

        javaFieldGetSet.setFieldInfo("\tprivate " + javaFieldType + " " + column.getName() + ";\n");

        //生成get方法的源代码
        StringBuilder getSrc = new StringBuilder();
        getSrc.append("\tpublic " + javaFieldType + " get" + StringUtils.firstChar2UpperCase(column.getName()) + "(){\n");
        getSrc.append("\t\treturn " + column.getName() + ";\n");
        getSrc.append("\t}\n");
        javaFieldGetSet.setGetInfo(getSrc.toString());

        //生成set方法的源代码
        StringBuilder setSrc = new StringBuilder();
        setSrc.append("\tpublic void set" + StringUtils.firstChar2UpperCase(column.getName()) + "(");
        setSrc.append(javaFieldType + " " + column.getName() + "){\n");
        setSrc.append("\t\tthis." + column.getName() + "=" + column.getName() + ";\n");
        setSrc.append("\t}\n");
        javaFieldGetSet.setSetInfo(setSrc.toString());

        return javaFieldGetSet;

    }

    /**
     * 根据表信息生成java类的源代码
     * @param tableInfo 表信息
     * @param convertor 数据类型转化器
     * @return java类源代码
     */
    public static String createJavaSrc(TableInfo tableInfo, TypeConvertor convertor) {
        StringBuilder src = new StringBuilder();

        Map<String, ColumnInfo> columns = tableInfo.getColumns();
        List<JavaFieldGetSet> javaFields = new ArrayList<>();

        for (ColumnInfo c: columns.values()) {
            javaFields.add(createFieldGetSetSRC(c, convertor));
        }

        //生成package语句
        src.append("package " + DBManager.getConf().getPoPackage() + ";\n\n");

        //生成import语句
        src.append("import java.sql.*;\n");
        src.append("import java.util.*;\n\n");

        //生成类声明语句
        src.append("public class " + StringUtils.firstChar2UpperCase(tableInfo.getTname()) + " " + "{\n\n");

        //生成属性列表
        for (JavaFieldGetSet f: javaFields) {
            src.append(f.getFieldInfo());
        }
        src.append("\n\n");

        //生成get方法列表
        for (JavaFieldGetSet f: javaFields) {
            src.append(f.getGetInfo());
        }

        //生成set方法列表
        for (JavaFieldGetSet f: javaFields) {
            src.append(f.getSetInfo());
        }

        //生成类结束
        src.append("}\n");
        //System.out.println(src);
        return src.toString();
    }

    public static void createJavaPOFile(TableInfo tableInfo, TypeConvertor convertor) {
        String src = createJavaSrc(tableInfo, convertor);

        String srcPath = DBManager.getConf().getSrcPath() + "\\";
        String packagePath = DBManager.getConf().getPoPackage().replaceAll("\\.", "/");

        File f= new File(srcPath + packagePath);
        //System.out.println(f.getAbsolutePath() + "***********");

        if (!f.exists()) {
            f.mkdirs();
        }

        BufferedWriter bw = null;

        try {
            bw = new BufferedWriter(new FileWriter(f.getAbsolutePath() + "/"+ StringUtils.firstChar2UpperCase(tableInfo.getTname()) + ".java"));
            bw.write(src);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null) {
                    bw.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
//        ColumnInfo ci = new ColumnInfo("id", "int", 0);
//        JavaFieldGetSet f = createFieldGetSetSRC(ci, new MySqlTypeConvertor());
//        System.out.println(f);

        Map<String, TableInfo> map = TableContext.tables;
        for (TableInfo t: map.values()) {
            createJavaPOFile(t, new MySqlTypeConvertor());
        }
    }
}
