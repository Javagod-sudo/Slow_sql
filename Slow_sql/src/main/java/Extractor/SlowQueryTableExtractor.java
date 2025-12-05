package Extractor;
import java.util.*;
import java.util.regex.*;
import java.util.stream.Collectors;
public class SlowQueryTableExtractor {

    // 超级正则：精准提取 SQL 中的表名（支持别名、JOIN、多库、子查询等）
    private static final Pattern TABLE_PATTERN = Pattern.compile(
            // 重点：只抓 FROM/JOIN/INSERT INTO 后面的第一个“单词”（就是表名！）
            "(?i)\\b(?:FROM|JOIN|INTO|UPDATE)\\s++(?:[\\w]+\\.)?([A-Z0-9_]{4,60})\\b|" +
                    "\\bINSERT\\s+INTO\\s+(?:[\\w]+\\.)?([A-Z0-9_]{4,60})\\b|" +
                    "\\b([A-Z0-9_]{4,60})\\s++[A-Z0-9_]+\\b",  // table alias 模式
            Pattern.CASE_INSENSITIVE
    );

    // 常见系统表和临时表关键字，自动过滤
    static Set<String> IGNORE_TABLES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "SELECT","FROM","JOIN","WHERE","AND","OR","ON","LEFT","RIGHT","INNER","OUTER",
            "INSERT","UPDATE","DELETE","INTO","AS","CASE","WHEN","THEN","ELSE","END",
            "NULL","UNION","ALL","DISTINCT","ORDER","BY","GROUP","HAVING"
    )));


    // ==================== 3. 核心方法：从一条 SQL 提取表名并返回逗号拼接字符串 ====================
    public static String extractTableNames(String sql) {
        if (sql == null || sql.trim().isEmpty()) return "";

        Set<String> tables = new LinkedHashSet<>();
        Matcher m = TABLE_PATTERN.matcher(sql.toUpperCase().replaceAll("[`\"'\\(\\)]", " ")); // 干掉引号和括号

        while (m.find()) {
            String table = m.group(1);  // FROM 后的表名
            if (table == null) table = m.group(2);  // INSERT INTO 后的
            if (table == null) table = m.group(3);  // table alias 模式

            if (table != null && table.matches("[A-Z0-9_]{4,60}")) {
                tables.add(table);
            }
        }
        return String.join(",", tables);
    }


    public static  List<Map<String, Object>>  filterTable (List<Map<String, Object>>  data,String fieldName  , String filterwords) {


        return data.stream()
                .filter(row -> {
                     Object value=row.get(fieldName);
                    return fieldName != null && value.equals(filterwords.toString().trim());
                }).collect(Collectors.toList());

    }

}