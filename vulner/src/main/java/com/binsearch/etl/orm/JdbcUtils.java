package com.binsearch.etl.orm;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class JdbcUtils {

    public enum ColumnClazz {
        STRING("String", String.class),
        INTEGER("Integer", Integer.class),
        DOUBLE("Double", Double.class),
        BOOLEAN("Boolean", Boolean.class),
        DATE("Date", Date.class),
        SHORT("Short", Short.class),
        LONG("Long", Long.class);

        private String property;
        private Class clazz;

        private ColumnClazz(String property, Class clazz) {
            this.property = property;
            this.clazz = clazz;
        }

        public static Class getColumnClazz(String clazz) {
            for (ColumnClazz resultCode : ColumnClazz.values()) {
                if (resultCode.property.equals(clazz)) {
                    return resultCode.clazz;
                }
            }
            return null;
        }
    }


    static class ColumnField {
        //是否为主键
        public boolean isId;
        //字段名
        public String columnName;
        //属性名
        public String fieldName;
        //属性类型
        public Class fieldClazzType;
        //属性Field对象
        public Field fieldObj;
    }

    static class EntityInfo {
        //字段信息
        public List<ColumnField> columnFields;
        //表名
        public String table;
        //实体类型
        public String entityType;
    }


    public enum OrderBy {
        DESC, ASC;
    }

    private OrderBy orderBy;

    private String[] orderByColumns;

    private JdbcTemplate jdbcTemplate;

    private TransactionTemplate transactionTemplate;

    //实体缓存
    private static final Map<String, EntityInfo> entityCache = new HashMap<String, EntityInfo>() {
    };

    //新表名
    private String tableName;


    //新列名
    private List<String> queryColumns;

    //表别名
    private String aliasTable;

    //条件语句
    private String where;

    //条件参数
    private Object[] whereProperty;

    //limit
    private String limit;

    //limit参数
    private Object[] limitProperty;

    //运行异常
    private Exception exception;

    //是否打印sql
    private static boolean PRINT_SQL = true;

    //结果集类型描述
    private String resultType;


    public static void closePrintSQL() {
        PRINT_SQL = false;
    }


    public JdbcUtils(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }


    public JdbcUtils(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }


    public JdbcUtils clear() {
        this.tableName = null;
        this.aliasTable = null;
        this.where = null;
        this.whereProperty = null;
        this.limit = null;
        this.limitProperty = null;
        this.exception = null;
        this.resultType = null;
        return this;
    }

    public <T> JdbcUtils model(Class<T> clazz) {
        if (Objects.isNull(clazz)) {
            this.exception = new Exception("模型沒有clazz为空");
            return this;
        }

        try {
            EntityInfo entityInfo = entityCache.get(clazz.getName());
            if (Objects.isNull(entityInfo)) {
                entityInfo = new EntityInfo();
                entityInfo.entityType = clazz.getName();
                //获取相关注解,获取表名
                Table table = clazz.getAnnotation(Table.class);
                if (Objects.nonNull(table)) {
                    entityInfo.table = table.name();
                } else {
                    entityInfo.table = clazz.getName().substring(clazz.getPackage().getName().length() + 1);
                    char[] charArray = entityInfo.table.toCharArray();
                    StringBuffer chars = new StringBuffer();
                    for (int i = 0; i < charArray.length; i++) {
                        if (i > 0 && charArray[i] >= 'A' && charArray[i] <= 'Z') {
                            chars.append('_');
                        }
                        chars.append(charArray[i]);
                    }
                    entityInfo.table = chars.toString().toLowerCase();
                }
                if (Strings.isEmpty(entityInfo.table)) {
                    this.exception = new Exception("表名为空");
                    return this;
                }

                //获取相关注解 或者表信息
                Field[] fields = clazz.getFields();
                ArrayList<ColumnField> columnFields = new ArrayList<ColumnField>() {
                };
                for (Field field : fields) {
                    if (field.isAnnotationPresent(Column.class)) {
                        Column column = field.getAnnotation(Column.class);
                        String genericType = field.getGenericType().toString();
                        columnFields.add(new ColumnField() {
                            {
                                fieldObj = field;
                                columnName = column.name();
                                fieldName = field.getName();
                                isId = field.isAnnotationPresent(Id.class);
                                fieldClazzType = ColumnClazz.getColumnClazz(genericType.substring(genericType.lastIndexOf(".") + 1));
                            }
                        });
                    }
                }

                if (CollectionUtils.isEmpty(columnFields)) {
                    this.exception = new Exception("columns信息为空");
                    return this;
                }

                entityInfo.columnFields = columnFields;
                entityCache.put(clazz.getName(), entityInfo);
            }
            this.resultType = entityInfo.entityType;
        } catch (Exception ex) {
            this.exception = new Exception(String.format("model方法出错,%s", ex.toString()));
        }
        return this;
    }


    public JdbcUtils table(String table) {
        if (Strings.isEmpty(table)) {
            this.exception = new Exception("table 不能为空");
            return this;
        }
        this.tableName = table;
        this.aliasTable = String.format("%s%d", this.tableName, (int) (1 + Math.random() * (100 - 1 + 1)));
        return this;
    }

    public JdbcUtils queryColumns(String... columns) {
        if (ArrayUtils.isEmpty(columns)) {
            this.exception = new Exception("columns 不能为空");
            return this;
        }
        this.queryColumns = Arrays.asList(columns);
        return this;
    }

    public JdbcUtils orderBy(OrderBy orderBy, String... columns) {
        if (ArrayUtils.isEmpty(columns)) {
            this.exception = new Exception("排序字段不能为空");
            return this;
        }

        this.orderByColumns = columns;
        this.orderBy = Objects.isNull(orderBy) ? OrderBy.DESC : orderBy;
        return this;
    }

    public JdbcUtils where(String where, Object... property) {
        if (Strings.isEmpty(where)) {
            this.exception = new Exception("语句条件为空");
            return this;
        }

        if (where.contains("where")) {
            this.exception = new Exception("存在多个where关键字");
            return this;
        }

        if (where.contains("?")) {
            if (ArrayUtils.isEmpty(property)) {
                this.exception = new Exception("where条件变量个数和参数个数不符合");
                return this;
            }

            String copy = new String(where);
            int num = copy.length();
            copy = copy.replace("?", "");
            num = num - copy.length();
            if (num != property.length) {
                this.exception = new Exception("where条件变量个数和参数个数不符合");
                return this;
            }
            this.whereProperty = property;
        }

        this.where = where.toLowerCase(Locale.ROOT);
        return this;
    }


    public JdbcUtils limit(Object... property) {
        if (ArrayUtils.isEmpty(property) || property.length > 2) {
            this.exception = new Exception("limit条件变量不能为空,或参数个数不对");
            return this;
        }

        if (property.length == 1) {
            this.limit = " limit ?";
        } else {
            this.limit = " limit ?,?";
        }

        this.limitProperty = property;
        return this;
    }


    public Exception error() {
        return this.exception;
    }


    public <T> JdbcUtils group(List<T> result, Class<T> resultClass, List<String> resultColumnNames, String... groupColumnNames) {
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (Objects.isNull(resultClass)) {
            this.exception = new Exception("模型信息为空");
            return this;
        }

        //获取
        this.model(resultClass);


        if (Strings.isEmpty(this.resultType)) {
            this.exception = new Exception("模型信息为空");
            return this;
        }

        if (!entityCache.containsKey(this.resultType)) {
            this.exception = new Exception("模型信息为空");
            return this;
        }

        EntityInfo entityInfo = entityCache.get(this.resultType);
        if (Objects.isNull(entityInfo)) {
            this.exception = new Exception("模型信息为空");
            return this;
        }

        if (CollectionUtils.isEmpty(resultColumnNames)) {
            this.exception = new Exception("结果返回列信息为空");
            return this;
        }

        if (ArrayUtils.isEmpty(groupColumnNames)) {
            this.exception = new Exception("分组列信息不能为空");
            return this;
        }

        if (Objects.isNull(result)) {
            this.exception = new Exception("结果容器不能为空");
            return this;
        }

        String clazz = this.resultType;
        RowMapper<Object> rowMapper = new RowMapper<Object>() {
            @Override
            public Object mapRow(ResultSet rs, int i) throws SQLException {
                try {
                    Class clz = Class.forName(clazz);
                    result.add(makeDbEntity(rs, (Class<? extends T>) clz));
                } catch (Exception e) {
                    throw new SQLException(e.getMessage());
                }
                return null;
            }
        };

        String str = "";
        //存放参数
        Object[] property = new Object[0];
        try {
            String table = entityInfo.table;
            if (Strings.isNotEmpty(this.tableName)) {
                table = this.tableName;
            }
            this.aliasTable = String.format("%s%d", table, (int) (1 + Math.random() * (100 - 1 + 1)));

            for (int i = 0; i < resultColumnNames.size(); i++) {
                for (ColumnField columnField : entityInfo.columnFields) {
                    if (resultColumnNames.get(i).equals(columnField.columnName)) {
                        resultColumnNames.set(i, String.format("%s.`%s`", this.aliasTable, columnField.columnName));
                        break;
                    }
                }
            }


            str = String.format("select %s from %s as %s", StringUtils.join(resultColumnNames.toArray(), ","), table, this.aliasTable);

            //构建条件
            if (Strings.isNotEmpty(this.where)) {
                //为条件添加别名 列名 (=,>,<,>=,<=,!=,is not null,is null,in,between,like) ?
                String[] wheres = this.where.split("\\s{1}(and|or){1}\\s{1}");

                for (String item : wheres) {
                    Pattern pattern = Pattern.compile("\\s*\\w+\\s*(>=|<=|<>|\\+=|!=|=|>|<|\\s+is not null|\\s+is null|\\s+in\\s*\\(|\\s+between|\\s+like)\\s*");
                    Matcher matcher = pattern.matcher(item);

                    while (matcher.find()) {
                        String tempstr = matcher.group().trim();
                        tempstr = tempstr.replaceAll("\\s*(>=|<=|<>|\\+=|!=|=|>|<|\\s+is not null|\\s+is null|\\s+in\\s*\\(|\\s+between|\\s+like)\\s*", "");
                        tempstr = item.replace(tempstr, String.format("%s.`%s`", this.aliasTable, tempstr.trim()));
                        this.where = this.where.replace(item, tempstr);
                    }
                }

                str = String.format("%s where %s", str, this.where);

                if (ArrayUtils.isNotEmpty(this.whereProperty)) {
                    property = ArrayUtils.addAll(property, this.whereProperty);
                }
            }

            //构建 limit
            if (Strings.isNotEmpty(this.limit)) {
                str = String.format("%s %s", str, this.limit);
                if (ArrayUtils.isNotEmpty(this.limitProperty)) {
                    property = ArrayUtils.addAll(property, this.limitProperty);
                }
            }

            //构建orderBy
            if (ArrayUtils.isNotEmpty(this.orderByColumns)) {
                String ocs = "";
                for (String oc : this.orderByColumns) {
                    ocs += String.format("%s.`%s` %s,", this.aliasTable, oc, this.orderBy);
                }
                ocs = ocs.substring(0, ocs.length() - 1);
                str = String.format("%s order by %s", str, ocs);
            }

            if (ArrayUtils.isNotEmpty(groupColumnNames)) {
                String gbs = "";
                for (String oc : groupColumnNames) {
                    gbs += String.format("%s.`%s`,", this.aliasTable, oc);
                }
                gbs = gbs.substring(0, gbs.length() - 1);
                str = String.format("%s group by %s", str, gbs);
            }

            //sql执行
            if (ArrayUtils.isEmpty(property)) {
                jdbcTemplate.query(str, rowMapper);
            } else {
                jdbcTemplate.query(str, property, rowMapper);
            }

        } catch (Exception e) {
            this.exception = new Exception(String.format("group by 查询错误,%s", e.getMessage()));
        }
        this.println(str, property, this.exception);
        return this;
    }


    public JdbcUtils count(EntityCountNum num) {
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (Objects.isNull(num)) {
            this.exception = new Exception("count 参数不能为空");
            return this;
        }

        String tableName = "";
        String[] columnsNames = new String[1];
        if (Strings.isEmpty(this.resultType)) {
            tableName = this.tableName;
            columnsNames[0] = "count(*)";
            this.aliasTable = String.format("%s%d", tableName, (int) (1 + Math.random() * (100 - 1 + 1)));
        } else {
            EntityInfo entityInfo = entityCache.get(this.resultType);
            tableName = entityInfo.table;
            this.aliasTable = String.format("%s%d", tableName, (int) (1 + Math.random() * (100 - 1 + 1)));
            for (ColumnField columnField : entityInfo.columnFields) {
                if (columnField.isId) {
                    columnsNames[0] = String.format("count(%s.`%s`)", this.aliasTable, columnField.columnName);
                    break;
                }
            }
        }


        Object[] property = new Object[0];
        String str = String.format("select %s from %s as %s", StringUtils.join(columnsNames, ","), tableName, this.aliasTable);

        //构建条件
        if (Strings.isNotEmpty(this.where)) {
            //为条件添加别名 列名 (=,>,<,>=,<=,!=,is not null,is null,in,between,like) ?
            String[] wheres = this.where.split("\\s{1}(and|or){1}\\s{1}");

            for (String item : wheres) {
                Pattern pattern = Pattern.compile("\\s*\\w+\\s*(>=|<=|<>|\\+=|!=|=|>|<|\\s+is not null|\\s+is null|\\s+in\\s*\\(|\\s+between|\\s+like)\\s*");
                Matcher matcher = pattern.matcher(item);

                while (matcher.find()) {
                    String tempstr = matcher.group().trim();
                    tempstr = tempstr.replaceAll("\\s*(>=|<=|<>|\\+=|!=|=|>|<|\\s+is not null|\\s+is null|\\s+in\\s*\\(|\\s+between|\\s+like)\\s*", "");
                    tempstr = item.replace(tempstr, String.format("%s.`%s`", this.aliasTable, tempstr.trim()));
                    this.where = this.where.replace(item, tempstr);
                }
            }

            str = String.format("%s where %s", str, this.where);

            if (ArrayUtils.isNotEmpty(this.whereProperty)) {
                property = ArrayUtils.addAll(property, this.whereProperty);
            }
        }

        //构建 limit
        if (Strings.isNotEmpty(this.limit)) {
            str = String.format("%s %s", str, this.limit);
            if (ArrayUtils.isNotEmpty(this.limitProperty)) {
                property = ArrayUtils.addAll(property, this.limitProperty);
            }
        }

        try {

            Integer temp = 0;
            if (ArrayUtils.isEmpty(property)) {
                temp = jdbcTemplate.queryForObject(str, num.getCountNum().getClass());
            } else {
                temp = jdbcTemplate.queryForObject(str, num.getCountNum().getClass(), property);
            }
            num.setCountNum(temp);
        } catch (Exception e) {
            this.exception = new Exception(String.format("count查询失败,%s", e.getMessage()));
        }
        this.println(str, property, this.exception);
        return this;
    }


    public <T> JdbcUtils query(String sql, Class<T> resultType, List<T> result, Object... property) {
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (Objects.isNull(result)) {
            this.exception = new Exception("querySQL 结果容器不能为空");
            return this;
        }

        if (Strings.isEmpty(sql)) {
            this.exception = new Exception("querySQL 语句不能为空");
            return this;
        }

        if (Objects.isNull(resultType)) {
            this.exception = new Exception("模型信息为空");
            return this;
        }

        //获取
        this.model(resultType);

        if (Strings.isEmpty(this.resultType)) {
            this.exception = new Exception("模型信息为空");
            return this;
        }

        if (!entityCache.containsKey(this.resultType)) {
            this.exception = new Exception("模型信息为空");
            return this;
        }

        if (Objects.nonNull(this.exception)) {
            return this;
        }

        //存放参数
        Object[] sqlPropertys = new Object[0];
        if (sql.contains("?")) {
            if (ArrayUtils.isEmpty(property)) {
                this.exception = new Exception("where条件变量个数和参数个数不符合");
                return this;
            }

            String copy = new String(sql);
            int num = copy.length();
            copy = copy.replace("?", "");
            num = num - copy.length();
            if (num != property.length) {
                this.exception = new Exception("where条件变量个数和参数个数不符合");
                return this;
            }
            sqlPropertys = property;
        }


        RowMapper<Object> rowMapper = new RowMapper<Object>() {
            @Override
            public Object mapRow(ResultSet rs, int i) throws SQLException {
                try {
                    result.add(makeDbEntity(rs, resultType));
                } catch (Exception e) {
                    throw new SQLException(e.getMessage());
                }
                return null;
            }
        };


        try {
            if (ArrayUtils.isEmpty(sqlPropertys)) {
                jdbcTemplate.query(sql, rowMapper);
            } else {
                jdbcTemplate.query(sql, sqlPropertys, rowMapper);
            }
        } catch (Exception e) {
            this.exception = new Exception(String.format("querySQL 查询错误,%s", e.getMessage()));
        }
        this.println(sql, property, this.exception);
        return this;
    }


    public <T> JdbcUtils query(List<T> result) {
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (Strings.isEmpty(this.resultType)) {
            this.exception = new Exception("模型信息为空");
            return this;
        }

        EntityInfo entityInfo = entityCache.get(this.resultType);
        if (Objects.isNull(entityInfo)) {
            this.exception = new Exception("模型信息为空");
            return this;
        }

        if (Objects.isNull(result)) {
            this.exception = new Exception("结果容器不能为空");
            return this;
        }

        String clazz = this.resultType;
        RowMapper<Object> rowMapper = new RowMapper<Object>() {
            @Override
            public Object mapRow(ResultSet rs, int i) throws SQLException {
                try {
                    Class clz = Class.forName(clazz);
                    result.add(makeDbEntity(rs, (Class<? extends T>) clz));
                } catch (Exception e) {
                    throw new SQLException(e.getMessage());
                }
                return null;
            }
        };

        String str = "";
        //存放参数
        Object[] property = new Object[0];
        try {
            StringBuilder temp = new StringBuilder();

            String table = entityInfo.table;
            if (Strings.isNotEmpty(this.tableName)) {
                table = this.tableName;
            }
            this.aliasTable = String.format("%s%d", table, (int) (1 + Math.random() * (100 - 1 + 1)));

            if (CollectionUtils.isEmpty(this.queryColumns)) {
                for (ColumnField columnField : entityInfo.columnFields) {
                    temp.append(String.format("%s.`%s`", this.aliasTable, columnField.columnName)).append(",");
                }
            } else {
                for (String column : this.queryColumns) {
                    temp.append(String.format("%s.`%s`", this.aliasTable, column)).append(",");
                }
            }
            str = temp.toString();
            str = String.format("select %s from %s as %s", str.substring(0, str.length() - 1), table, this.aliasTable);

            //构建条件
            if (Strings.isNotEmpty(this.where)) {
                //为条件添加别名 列名 (=,>,<,>=,<=,!=,is not null,is null,in,between,like) ?
                String[] wheres = this.where.split("\\s{1}(and|or){1}\\s{1}");

                for (String item : wheres) {
                    Pattern pattern = Pattern.compile("\\s*\\w+\\s*(>=|<=|<>|\\+=|!=|=|>|<|\\s+is not null|\\s+is null|\\s+in\\s*\\(|\\s+between|\\s+like)\\s*");
                    Matcher matcher = pattern.matcher(item);

                    while (matcher.find()) {
                        String tempstr = matcher.group().trim();
                        tempstr = tempstr.replaceAll("\\s*(>=|<=|<>|\\+=|!=|=|>|<|\\s+is not null|\\s+is null|\\s+in\\s*\\(|\\s+between|\\s+like)\\s*", "");
                        tempstr = item.replace(tempstr, String.format("%s.`%s`", this.aliasTable, tempstr.trim()));
                        this.where = this.where.replace(item, tempstr);
                    }
                }

                str = String.format("%s where %s", str, this.where);

                if (ArrayUtils.isNotEmpty(this.whereProperty)) {
                    property = ArrayUtils.addAll(property, this.whereProperty);
                }
            }

            //构建 limit
            if (Strings.isNotEmpty(this.limit)) {
                str = String.format("%s %s", str, this.limit);
                if (ArrayUtils.isNotEmpty(this.limitProperty)) {
                    property = ArrayUtils.addAll(property, this.limitProperty);
                }
            }

            //构建orderBy
            if (ArrayUtils.isNotEmpty(this.orderByColumns)) {
                String ocs = "";
                for (String oc : this.orderByColumns) {
                    ocs += String.format("%s.`%s` %s,", this.aliasTable, oc, this.orderBy);
                }
                ocs = ocs.substring(ocs.length() - 1);
                str = String.format("%s order by %s", str, ocs);
            }

            //sql执行
            if (ArrayUtils.isEmpty(property)) {
                jdbcTemplate.query(str, rowMapper);
            } else {
                jdbcTemplate.query(str, property, rowMapper);
            }

        } catch (Exception e) {
            this.exception = new Exception(String.format("querySQL 查询错误,%s", e.getMessage()));
        }
        this.println(str, property, this.exception);
        return this;
    }


    public <T> T makeDbEntity(ResultSet rs, Class<T> clazz) throws Exception {
        T object = clazz.newInstance();
        Field[] fields = clazz.getDeclaredFields();

        int columnCount = rs.getMetaData().getColumnCount();
        int columnNum = 1;
        while (columnNum <= columnCount) {
            try {
                String columnName = rs.getMetaData().getColumnName(columnNum);
                Object value = rs.getObject(columnNum);

                Field field = null;
                for (Field temp : fields) {
                    if (temp.isAnnotationPresent(Column.class)) {
                        Column column = temp.getAnnotation(Column.class);
                        if (column.name().equals(columnName)) {
                            field = temp;
                            break;
                        }
                    }
                }

                if (Objects.nonNull(field) && Objects.nonNull(value)) {
                    field.setAccessible(true);
                    if (field.getType() == value.getClass()) {
                        field.set(object, value);
                    } else if (field.getType().isAssignableFrom(value.getClass())) {
                        field.set(object, value);
                    } else {
                        String strValue = String.valueOf(value);
                        Class<?> threadClazz = null;
                        if (field.getType() == Integer.class) {
                            threadClazz = Integer.class;
                        } else if (field.getType() == Long.class) {
                            threadClazz = Long.class;
                        } else if (field.getType() == Short.class) {
                            threadClazz = Short.class;
                        } else if (field.getType() == Boolean.class) {
                            threadClazz = Boolean.class;
                            if (strValue.equals("0") || strValue.equals("1")) {
                                strValue = strValue.equals("0") ? "false" : "true";
                            }
                        } else if (field.getType() == Double.class) {
                            threadClazz = Double.class;
                        } else if (field.getType() == Float.class) {
                            threadClazz = Float.class;
                        }

                        if (!ObjectUtils.isEmpty(threadClazz)) {
                            Method method = threadClazz.getMethod("valueOf", String.class);
                            field.set(object, method.invoke(null, strValue));
                        }
                    }
                }
            } catch (Exception e) {
                throw e;
            }
            columnNum++;
        }

        return object;
    }


    private void println(String str, Object[] property, Exception exception) {
        if (PRINT_SQL) {
            String propertyStr = "";
            if (Objects.nonNull(property)) {
                propertyStr = JSON.toJSONString(property, SerializerFeature.PrettyFormat);
            }

            String exceptionStr = "";
            if (Objects.nonNull(exception)) {
                exceptionStr = exception.getMessage();
            }
            System.out.println(String.format("================================== \n***** %s\n***** %s\n***** %s\n", str, propertyStr, exceptionStr));
        }
    }


    public JdbcUtils create(Object entity) {
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (Objects.isNull((entity))) {
            this.exception = new Exception("数据不能为空");
            return this;
        }

        this.model(entity.getClass());
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (Strings.isEmpty(this.resultType)) {
            this.exception = new Exception("class 信息不能为空");
            return this;
        }

        EntityInfo entityInfo = entityCache.get(this.resultType);
        if (Objects.isNull(entityInfo)) {
            this.exception = new Exception("class 信息不能为空");
            return this;
        }

        String table = entityInfo.table;
        if (Strings.isNotEmpty(this.tableName)) {
            table = this.tableName;
        }

        String sql = "";
        Object[] temp = null;
        try {
            StringBuilder columnBuilder = new StringBuilder();
            StringBuilder columnsValueBuilder = new StringBuilder();
            List<Object> property = new ArrayList<Object>() {
            };
            Field propertyId = null;
            for (ColumnField field : entityInfo.columnFields) {
                columnBuilder.append(field.columnName).append(",");
                columnsValueBuilder.append("?,");
                property.add(field.fieldObj.get(entity));
                if (field.isId) {
                    propertyId = field.fieldObj;
                }
            }
            String columns = columnBuilder.toString();
            columns = columns.substring(0, columns.length() - 1);

            String columnsValue = columnsValueBuilder.toString();
            columnsValue = columnsValue.substring(0, columnsValue.length() - 1);


            sql = String.format("insert into %s(%s)values(%s)", table, columns, columnsValue);
            temp = property.toArray();

            String finalSql = sql;
            Object[] finalTemp = temp;
            KeyHolder keyHolder = new GeneratedKeyHolder();
            jdbcTemplate.update(new PreparedStatementCreator() {
                public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                    int num = 1;
                    PreparedStatement ps = connection.prepareStatement(finalSql, Statement.RETURN_GENERATED_KEYS);

                    for (Object obj : finalTemp) {
                        ps.setObject(num, obj);
                        num++;
                    }
                    return ps;
                }
            }, keyHolder);

            if (Objects.nonNull(propertyId)) {
                if (Objects.nonNull(keyHolder.getKey())) {
                    String generatedId = keyHolder.getKey().toString();

                    Class<?> threadClazz = null;
                    if (propertyId.getType() == Integer.class) {
                        threadClazz = Integer.class;
                    } else if (propertyId.getType() == Long.class) {
                        threadClazz = Long.class;
                    } else {
                        threadClazz = String.class;
                    }
                    Method method = threadClazz.getMethod("valueOf", String.class);
                    propertyId.set(entity, method.invoke(null, generatedId));
                }
            }
        } catch (Exception e) {
            //回滚
            this.exception = e;
            this.println(sql, temp, this.exception);
        }
        return this;
    }


    public JdbcUtils batchCreate(Object... entitys) {
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (ArrayUtils.isEmpty(entitys)) {
            this.exception = new Exception("数据不能为空");
            return this;
        }

        this.model(entitys[0].getClass());
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (Strings.isEmpty(this.resultType)) {
            this.exception = new Exception("模型信息不能为空");
            return this;
        }

        EntityInfo entityInfo = entityCache.get(this.resultType);
        if (Objects.isNull(entityInfo)) {
            this.exception = new Exception("模型信息不能为空");
            return this;
        }

        String table = entityInfo.table;
        if (Strings.isNotEmpty(this.tableName)) {
            table = this.tableName;
        }


        StringBuilder columnBuilder = new StringBuilder();
        StringBuilder columnsValueBuilder = new StringBuilder();

        for (ColumnField field : entityInfo.columnFields) {
            columnBuilder.append(field.columnName).append(",");
            columnsValueBuilder.append("?,");
        }
        String columns = columnBuilder.toString();
        columns = columns.substring(0, columns.length() - 1);

        String columnsValue = columnsValueBuilder.toString();
        columnsValue = columnsValue.substring(0, columnsValue.length() - 1);


        String sql = String.format("insert into %s(%s)values(%s)", table, columns, columnsValue);

        List<Object[]> batchArgs = Arrays.stream(entitys).map(item -> {
            List<Object> property = new ArrayList<Object>() {
            };
            for (ColumnField field : entityInfo.columnFields) {
                try {
                    property.add(field.fieldObj.get(item));
                } catch (Exception e) {
                    this.exception = e;
                    break;
                }
            }
            return Objects.nonNull(this.exception) ? null : property.toArray();
        }).collect(Collectors.toList());

        try {
            if (Objects.isNull(this.exception)) {
                jdbcTemplate.batchUpdate(sql, batchArgs);
            }
        } catch (Exception e) {
            this.exception = e;
        } finally {
            this.println(sql, null, this.exception);
        }
        return this;
    }


    public JdbcUtils batchUpdate(Object... entitys) {
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (ArrayUtils.isEmpty(entitys)) {
            this.exception = new Exception("数据不能为空");
            return this;
        }

        this.model(entitys[0].getClass());
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (Strings.isEmpty(this.resultType)) {
            this.exception = new Exception("模型信息不能为空");
            return this;
        }

        EntityInfo entityInfo = entityCache.get(this.resultType);
        if (Objects.isNull(entityInfo)) {
            this.exception = new Exception("模型信息不能为空");
            return this;
        }

        String table = entityInfo.table;
        if (Strings.isNotEmpty(this.tableName)) {
            table = this.tableName;
        }

        String sql = "";
        String columnId = "";

        StringBuilder columnbuilder = new StringBuilder();
        for (ColumnField field : entityInfo.columnFields) {
            if (!field.isId) {
                columnbuilder.append(field.columnName).append("=?").append(",");
                continue;
            }
            columnId = field.columnName + "=?";
        }
        String columns = columnbuilder.toString();
        columns = columns.substring(0, columns.length() - 1);

        sql = String.format("update %s set %s", table, columns);

        if (Strings.isNotEmpty(this.where)) {
            sql = String.format("%s where %s", sql, this.where);
        } else if (Strings.isNotEmpty(columnId)) {
            sql = String.format("%s where %s", sql, columnId);
        }


        String finalColumnId = columnId;
        List<Object[]> batchArgs = Arrays.stream(entitys).map(item -> {
            Object propertyId = null;
            List<Object> property = new ArrayList<Object>() {
            };
            for (ColumnField field : entityInfo.columnFields) {
                try {
                    if (!field.isId) {
                        property.add(field.fieldObj.get(item));
                        continue;
                    }
                    propertyId = field.fieldObj.get(item);
                } catch (Exception e) {
                    this.exception = e;
                    break;
                }
            }

            if (Objects.nonNull(this.exception)) {
                return null;
            }

            Object[] temp = property.toArray();
            if (Strings.isNotEmpty(this.where) &&
                    ArrayUtils.isNotEmpty(this.whereProperty)) {
                temp = ArrayUtils.addAll(temp, this.whereProperty);
            } else if (Strings.isNotEmpty(finalColumnId) && Objects.nonNull(propertyId)) {
                temp = ArrayUtils.addAll(temp, propertyId);
            }
            return temp;
        }).collect(Collectors.toList());

        try {
            if (Objects.isNull(this.exception)) {
                jdbcTemplate.batchUpdate(sql, batchArgs);
            }
        } catch (Exception e) {
            this.exception = e;
        } finally {
            this.println(sql, null, this.exception);
        }
        return this;
    }


    public JdbcUtils batchUpdate(String sqlColumns, Object... entitys) {
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (Strings.isEmpty(sqlColumns)) {
            this.exception = new Exception("set 字段不能为空");
            return this;
        }

        if (ArrayUtils.isEmpty(entitys)) {
            this.exception = new Exception("数据不能为空");
            return this;
        }

        this.model(entitys[0].getClass());
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (Strings.isEmpty(this.resultType)) {
            this.exception = new Exception("模型信息不能为空");
            return this;
        }

        EntityInfo entityInfo = entityCache.get(this.resultType);
        if (Objects.isNull(entityInfo)) {
            this.exception = new Exception("模型信息不能为空");
            return this;
        }

        String table = entityInfo.table;
        if (Strings.isNotEmpty(this.tableName)) {
            table = this.tableName;
        }


        String columnId = "";
        for (ColumnField field : entityInfo.columnFields) {
            if (field.isId) {
                columnId = field.columnName + "=?";
                break;
            }
        }

        String sql = String.format("update %s set %s", table, sqlColumns);

        if (Strings.isNotEmpty(this.where)) {
            sql = String.format("%s where %s", sql, this.where);
        } else if (Strings.isNotEmpty(columnId)) {
            sql = String.format("%s where %s", sql, columnId);
        }


        String finalColumnId = columnId;
        List<Object[]> batchArgs = Arrays.stream(entitys).map(item -> {
            Object propertyId = null;
            List<Object> property = new ArrayList<Object>() {
            };
            for (ColumnField field : entityInfo.columnFields) {
                try {
                    if (!field.isId && sqlColumns.contains(field.columnName)) {
                        property.add(field.fieldObj.get(item));
                        continue;
                    }
                    propertyId = field.fieldObj.get(item);

                } catch (Exception e) {
                    this.exception = e;
                    break;
                }
            }

            if (Objects.nonNull(this.exception)) {
                return null;
            }

            Object[] temp = property.toArray();
            if (Strings.isNotEmpty(this.where) &&
                    ArrayUtils.isNotEmpty(this.whereProperty)) {
                temp = ArrayUtils.addAll(temp, this.whereProperty);
            } else if (Strings.isNotEmpty(finalColumnId) && Objects.nonNull(propertyId)) {
                temp = ArrayUtils.addAll(temp, propertyId);
            }
            return temp;
        }).collect(Collectors.toList());

        try {
            if (Objects.isNull(this.exception)) {
                jdbcTemplate.batchUpdate(sql, batchArgs);
            }
        } catch (Exception e) {
            this.exception = e;
        } finally {
            this.println(sql, null, this.exception);
        }
        return this;
    }


    public JdbcUtils update(Object entity) {
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (Objects.isNull(entity)) {
            this.exception = new Exception("数据不能为空");
            return this;
        }

        this.model(entity.getClass());
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (Strings.isEmpty(this.resultType)) {
            this.exception = new Exception("模型信息不能为空");
            return this;
        }

        EntityInfo entityInfo = entityCache.get(this.resultType);
        if (Objects.isNull(entityInfo)) {
            this.exception = new Exception("模型信息不能为空");
            return this;
        }

        String table = entityInfo.table;
        if (Strings.isNotEmpty(this.tableName)) {
            table = this.tableName;
        }

        String sql = "";
        Object[] temp = null;
        try {
            StringBuilder columnbuilder = new StringBuilder();
            List<Object> property = new ArrayList<Object>() {
            };
            String columnId = "";
            Object propertyId = null;
            for (ColumnField field : entityInfo.columnFields) {
                if (!field.isId) {
                    columnbuilder.append(field.columnName).append("=?").append(",");
                    property.add(field.fieldObj.get(entity));
                    continue;
                }
                columnId = field.columnName + "=?";
                propertyId = field.fieldObj.get(entity);
            }
            String columns = columnbuilder.toString();
            columns = columns.substring(0, columns.length() - 1);

            sql = String.format("update %s set %s", table, columns);

            temp = property.toArray();
            if (Strings.isNotEmpty(this.where)) {
                sql = String.format("%s where %s", sql, this.where);
                if (ArrayUtils.isNotEmpty(this.whereProperty)) {
                    temp = ArrayUtils.addAll(temp, this.whereProperty);
                }
            } else if (Strings.isNotEmpty(columnId) && Objects.nonNull(propertyId)) {
                sql = String.format("%s where %s", sql, columnId);
                temp = ArrayUtils.addAll(temp, propertyId);
            }

            jdbcTemplate.update(sql, temp);
        } catch (Exception e) {
            this.exception = e;
        } finally {
            this.println(sql, temp, this.exception);
        }
        return this;
    }


    public JdbcUtils update(String sql, Object... property) {
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        //存放参数
        Object[] sqlPropertys = new Object[0];
        if (sql.contains("?")) {
            if (ArrayUtils.isEmpty(property)) {
                this.exception = new Exception("变量个数和参数个数不符合");
                return this;
            }

            String copy = new String(sql);
            int num = copy.length();
            copy = copy.replace("?", "");
            num = num - copy.length();
            if (num != property.length) {
                this.exception = new Exception("变量个数和参数个数不符合");
                return this;
            }
            sqlPropertys = property;
        }

        if (Strings.isNotEmpty(this.tableName)) {
            sql = String.format("update %s set %s ", this.tableName, sql);
        } else {
            if (Strings.isEmpty(this.resultType)) {
                this.exception = new Exception("class 信息不能为空");
                return this;
            }

            EntityInfo entityInfo = entityCache.get(this.resultType);
            if (Objects.isNull(entityInfo)) {
                this.exception = new Exception("class 信息不能为空");
                return this;
            }

            sql = String.format("update %s set %s ", entityInfo.table, sql);
        }

        if (Strings.isNotEmpty(this.where)) {
            sql = String.format("%s where %s", sql, this.where);
            if (ArrayUtils.isNotEmpty(this.whereProperty)) {
                sqlPropertys = ArrayUtils.addAll(sqlPropertys, this.whereProperty);
            }
        }

        try {
            jdbcTemplate.update(sql, sqlPropertys);
        } catch (Exception e) {
            this.exception = e;
        }
        this.println(sql, sqlPropertys, this.exception);
        return this;
    }


    public JdbcUtils delete(Object entity) {
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (Objects.isNull(entity)) {
            this.exception = new Exception("数据不能为空");
            return this;
        }

        this.model(entity.getClass());
        if (Objects.nonNull(this.exception)) {
            return this;
        }

        if (Strings.isEmpty(this.resultType)) {
            this.exception = new Exception("模型信息不能为空");
            return this;
        }

        EntityInfo entityInfo = entityCache.get(this.resultType);
        if (Objects.isNull(entityInfo)) {
            this.exception = new Exception("模型信息不能为空");
            return this;
        }

        String table = entityInfo.table;
        if (Strings.isNotEmpty(this.tableName)) {
            table = this.tableName;
        }

        String sql = "";
        Object[] temp = null;
        try {
            String columnId = "";
            Object propertyId = null;
            List<Object> property = new ArrayList<Object>() {
            };
            for (ColumnField field : entityInfo.columnFields) {
                if (!field.isId) {
                    continue;
                }
                columnId = field.columnName + "=?";
                propertyId = field.fieldObj.get(entity);
            }

            sql = String.format("delete from %s", table);

            temp = property.toArray();
            if (Strings.isNotEmpty(this.where)) {
                sql = String.format("%s where %s", sql, this.where);
                if (ArrayUtils.isNotEmpty(this.whereProperty)) {
                    temp = ArrayUtils.addAll(temp, this.whereProperty);
                }
            } else if (Strings.isNotEmpty(columnId) && Objects.nonNull(propertyId)) {
                sql = String.format("%s where %s", sql, columnId);
                temp = ArrayUtils.addAll(temp, propertyId);
            }

            jdbcTemplate.update(sql, temp);
        } catch (Exception e) {
            this.exception = e;
        } finally {
            this.println(sql, temp, this.exception);
        }
        return this;
    }



    public JdbcUtils transactionExecute(Transaction p){

        if(Objects.isNull(this.transactionTemplate)){
            this.exception = new Exception("事务对象不存在");
            return this;
        }

        final JdbcUtils jdbcUtils = this;
        this.transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                 try{
                    p.execute();
                    if(Objects.nonNull(jdbcUtils.exception)){
                        throw jdbcUtils.exception;
                    }
                 } catch (Exception e){
                    //回滚
                    status.setRollbackOnly();
                 }
            }
        });
        return this;
    }
}

interface Transaction{
    public void execute()throws Exception;
}



    /*
     *  执行带有返回值<Object>的事务管理
     */
//        this.transactionTemplate.execute(new TransactionCallback<Object>() {
//            @Override
//            public Object doInTransaction(TransactionStatus transactionStatus) {
//                try {
//                    //  ...
//                    //.......   业务代码
//                    return new Object();
//                } catch (Exception e) {
//                    //回滚
//                    transactionStatus.setRollbackOnly();
//                    return null;
//                }
//            }
//        });



//        KeyHolder keyHolder = new GeneratedKeyHolder();
//        jdbcTemplate.update(new PreparedStatementCreator() {
//            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
//
//                PreparedStatement ps = connection.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);
//                ps.setString(1, student .getTitle());
//                ps.setString(2, student .getContent());
//                ps.setString(3, student .getForm());
//                ps.setString(4, student .getSffs());
//                ps.setString(5, new Date(student.getTime().getTime()));
//                ps.setString(6, student .getBy2());
//                ps.setString(7, student .getBy3());
//                return ps;
//            }
//        }, keyHolder);
//
//        Long generatedId = keyHolder.getKey().longValue();
//        return generatedId;



//    public JdbcUtils update(Object... entitys){
//        if(Objects.nonNull(this.exception)){
//            return this;
//        }
//
//        if(ArrayUtils.isEmpty(entitys)){
//            this.exception = new Exception("update 结果容器不能为空");
//            return this;
//        }
//
//
//        if(Objects.isNull(this.transactionTemplate)){
//            this.exception = new Exception("update 事务管理器为空");
//            return this;
//        }
//
//        //没有返回值
//        final JdbcUtils jdbcUtils = this;
//        this.transactionTemplate.execute(new TransactionCallbackWithoutResult() {
//            @Override
//            protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
//                for(Object entity:entitys){
//                    String sql = "";
//                    Object[] temp = null;
//                    try{
//
//                        jdbcUtils.model(entity.getClass());
//
//                        if(Strings.isEmpty(jdbcUtils.resultType)){
//                            jdbcUtils.exception = new Exception("class 信息不能为空");
//                            throw  jdbcUtils.exception;
//                        }
//
//                        EntityInfo entityInfo = entityCache.get(jdbcUtils.resultType);
//                        if(Objects.isNull(entityInfo)){
//                            jdbcUtils.exception = new Exception("class 信息不能为空");
//                            throw jdbcUtils.exception;
//                        }
//
//                        if(Objects.nonNull(jdbcUtils.exception)){
//                            throw jdbcUtils.exception;
//                        }
//
//                        String table = entityInfo.table;
//                        if(Strings.isNotEmpty(jdbcUtils.tableName)){
//                            table = jdbcUtils.tableName;
//                        }
//
//                        StringBuilder columnbuilder = new StringBuilder();
//                        List<Object> property = new ArrayList<Object>(){};
//                        String columnId = "";
//                        Object propertyId = null;
//                        for(ColumnField field:entityInfo.columnFields){
//                            if(!field.isId) {
//                                columnbuilder.append(field.columnName).append("=?").append(",");
//                                property.add(field.fieldObj.get(entity));
//                                continue;
//                            }
//                            columnId = field.columnName + "=?";
//                            propertyId = field.fieldObj.get(entity);
//                        }
//                        String columns = columnbuilder.toString();
//                        columns = columns.substring(0,columns.length()-1);
//
//                        sql = String.format("update %s set %s",table,columns);
//
//                        temp = property.toArray();
//                        if(Strings.isNotEmpty(jdbcUtils.where)){
//                            sql = String.format("%s where %s",sql,jdbcUtils.where);
//                            if(ArrayUtils.isNotEmpty(jdbcUtils.whereProperty)){
//                                temp = ArrayUtils.addAll(temp,jdbcUtils.whereProperty);
//                            }
//                        }else if(Strings.isNotEmpty(columnId) && Objects.nonNull(propertyId)){
//                            sql = String.format("%s where %s",sql,columnId);
//                            temp = ArrayUtils.addAll(temp,propertyId);
//                        }
//
//                        jdbcTemplate.update(sql,temp);
//                    } catch (Exception e){
//                        //回滚
//                        jdbcUtils.exception = e;
//                        jdbcUtils.println(sql,temp,jdbcUtils.exception);
//                        transactionStatus.setRollbackOnly();
//                    }
//                }
//            }
//        });
//        return this;
//    }