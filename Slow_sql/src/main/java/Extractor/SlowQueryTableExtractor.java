package Extractor;
import org.apache.flink.types.StringValue;

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

    public static List<Map<String, Object>> unfilterTable(List<Map<String, Object>> data, String fieldName, String filterwords) {
        return data.stream()
                .filter(row -> {
                    Object value = row.get(fieldName);
                    // 修改为不等于：使用 !equals
                    return value != null && !value.toString().trim().equals(filterwords.trim());
                })
                .collect(Collectors.toList());
    }



    /**
     * 从SQL语句中提取表名并展开成多行记录
     *
     * @param dataList 原始数据列表
     * @return 新的数据列表，只包含两列：内部ID 和 table_name，一个表名对应一行
     */
    public static List<Map<String, Object>> extractTableNamesExpanded(List<Map<String, Object>> dataList,String nbid,String sqlfield) {
        List<Map<String, Object>> result = new ArrayList<>();

        String regex = "(?i)(?:from|join|update|into|delete\\s+from|create\\s+(?:or\\s+replace\\s+)?(?:table|view|materialized\\s+view)|drop\\s+(?:table|view|materialized\\s+view|index)|truncate\\s+table|alter\\s+table|insert\\s+into)\\s+(?:if\\s+(?:not\\s+)?exists\\s+)?([a-z_][a-z0-9_]*)";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

        for (Map<String, Object> row : dataList) {
            // 修正：从 row 中获取字段值
            String id = String.valueOf(row.get(nbid));
            String sqlText = (String) row.get(sqlfield);

            if (sqlText != null && !sqlText.trim().isEmpty()) {
                Matcher matcher = pattern.matcher(sqlText);


                while (matcher.find()) {
                    String tableName = matcher.group(1);
                    if (tableName != null && !tableName.trim().isEmpty()) {
                        // 剔除 FMP_QUERY_DB. 前缀
                        tableName = tableName.replace("FMP_QUERY_DB.", "");

                        // 创建新行，只包含 id 和 table_name 两列
                        Map<String, Object> newRow = new HashMap<>();
                        newRow.put("内部ID", id);
                        newRow.put("table_name", tableName);

                        result.add(newRow);
                    }
                }
            }
        }

        return result;
    }


    /**
     * 根据SQL语句内容判断财务专业类型
     *
     * @param dataList 原始数据列表，需包含"原始语句"字段
     * @return 新的数据列表，增加"财务专业"字段
     */
    public static List<Map<String, Object>> addFinancialType(List<Map<String, Object>> dataList) {
        List<Map<String, Object>> result = new ArrayList<>();

        for (Map<String, Object> row : dataList) {
            // 创建新的Map，复制原有数据
            Map<String, Object> newRow = new LinkedHashMap<>(row);

            String sqlText = (String) row.get("原始语句");
            String type = null;

            if (sqlText != null && !sqlText.trim().isEmpty()) {
                String upperSql = sqlText.toUpperCase();
                String lowerSql = sqlText.toLowerCase();

                // 按照优先级顺序判断
                if (upperSql.contains("TZZX_")) {
                    type = "提质增效";
                } else if (upperSql.contains("ZHCX_")) {
                    type = "综合查询";
                } else if (upperSql.contains("ADS_FIN_0201_LEDGER_XF_MF") ||
                        upperSql.contains("ADS_FIN_0201_YSTZ_MXKB")) {
                    type = "往来司库上报";
                } else if (upperSql.contains("DIM_ITG_1005_FINANCIAL_USER")) {
                    type = "运营看板-仪表板";
                } else if (upperSql.contains("FCT_FIN_0211_MAT_OPERATION_BOARD_INDEX")) {
                    type = "运营看板-首页指标";
                } else if (upperSql.contains("MRMP_") && !upperSql.matches(".*ODS.*_MRMP_.*")) {
                    type = "台账-往来";
                } else if ((upperSql.contains("ODS_WL_") && upperSql.contains("ADS_FIN_GLM_ACC")) ||
                        (upperSql.contains("ADS_FIN_GLM_ACC") && upperSql.contains("ODS_WL_"))) {
                    type = "总账+往来核对";
                } else if (upperSql.contains("DELETE") && upperSql.contains("ADS_FIN_GLM_ACC") &&
                        !upperSql.contains("ODS_WL_")) {
                    type = "总账删除";
                } else if (upperSql.contains("SELECT") && upperSql.contains("ADS_FIN_GLM_ACC") &&
                        !upperSql.contains("ODS_WL_")) {
                    type = "总账查询";
                } else if (upperSql.contains("UPDATE") && upperSql.contains("ADS_FIN_GLM_ACC") &&
                        !upperSql.contains("ODS_WL_")) {
                    type = "总账更新";
                } else if (upperSql.contains("SELECT") && upperSql.contains("ODS_WL_") &&
                        !upperSql.contains("ADS_FIN_GLM_ACC")) {
                    type = "往来查询";
                } else if (upperSql.contains("DELETE") && upperSql.contains("ODS_WL_") &&
                        !upperSql.contains("ADS_FIN_GLM_ACC")) {
                    type = "往来删除";
                } else if (upperSql.contains("UPDATE") && upperSql.contains("ODS_WL_") &&
                        !upperSql.contains("ADS_FIN_GLM_ACC")) {
                    type = "往来更新";
                } else if (lowerSql.contains("yykb") ||
                        lowerSql.contains("fct_fin_0200_bs_business") ||
                        lowerSql.contains("fct_fin_0200_bs_app")) {
                    type = "运营看板";
                } else if (upperSql.matches(".* ODS_.*") ||
                        upperSql.matches(".* DIM_.*") ||
                        upperSql.matches(".* FCT.*") ||
                        upperSql.matches(".* DWS_.*") ||
                        upperSql.matches(".* ADS_.*") ||
                        upperSql.contains("ELEC_")) {
                    type = "台账";
                }
            }

            // 添加财务专业字段
            newRow.put("财务专业", type);
            result.add(newRow);
        }

        return result;
    }

    /**
     * 按指定字段分组，将另一个字段的值去重并用逗号拼接
     *
     * @param dataList 原始数据列表
     * @param groupByField 分组字段名
     * @param concatField 需要拼接的字段名
     * @return 分组并拼接后的结果列表
     */
    public static List<Map<String, Object>> groupAndConcat(
            List<Map<String, Object>> dataList,
            String groupByField,
            String concatField) {

        // 使用 LinkedHashMap 保持插入顺序
        Map<String, Map<String, Object>> groupMap = new LinkedHashMap<>();
        Map<String, Set<String>> concatMap = new LinkedHashMap<>();

        for (Map<String, Object> row : dataList) {
            String groupKey = String.valueOf(row.get(groupByField));
            String concatValue = String.valueOf(row.get(concatField));

            // 如果是第一次遇到这个分组键，保存整行数据
            if (!groupMap.containsKey(groupKey)) {
                groupMap.put(groupKey, new LinkedHashMap<>(row));
                concatMap.put(groupKey, new LinkedHashSet<>());
            }

            // 添加到去重集合中（LinkedHashSet 保持插入顺序）
            if (concatValue != null && !"null".equals(concatValue) && !concatValue.trim().isEmpty()) {
                concatMap.get(groupKey).add(concatValue);
            }
        }

        // 构建结果
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> entry : groupMap.entrySet()) {
            String groupKey = entry.getKey();
            Map<String, Object> rowData = entry.getValue();

            // 将去重后的值用逗号拼接
            Set<String> values = concatMap.get(groupKey);
            String concatenated = String.join(",", values);

            // 更新拼接字段的值
            rowData.put(concatField, concatenated);
            result.add(rowData);
        }

        return result;
    }



    /**
     * 根据财务专业类型新增部门相关字段
     *
     * @param dataList 数据列表，需包含"财务专业"字段
     * @return 新增 dp、dp_leader、dp_group 字段后的数据列表
     */
    public static List<Map<String, Object>> addDeptByFinancialType(List<Map<String, Object>> dataList) {
        List<Map<String, Object>> result = new ArrayList<>();

        for (Map<String, Object> row : dataList) {
            Map<String, Object> newRow = new LinkedHashMap<>(row);

            String type = (String) row.get("财务专业");

            // 根据财务专业类型判断并新增字段
            if (type != null && type.contains("往来")) {
                newRow.put("dp", "DAP5");
                newRow.put("dp_leader", "王媛锋");
                newRow.put("dp_group", "应收应付项目组");
            } else if (type != null && type.contains("总账")) {
                newRow.put("dp", "DAP1");
                newRow.put("dp_leader", "刘洋");
                newRow.put("dp_group", "账务查询项目组");
            } else {
                // 其他情况设置为 null 或空字符串
                newRow.put("dp", null);
                newRow.put("dp_leader", null);
                newRow.put("dp_group", null);
            }

            result.add(newRow);
        }

        return result;
    }

    public static  List<Map<String, Object>> processFinalResult(List<Map<String, Object>> finalResult) {
        if (finalResult == null || finalResult.isEmpty()) {
            return finalResult;
        }

        for (Map<String, Object> row : finalResult) {
            // 获取"格式化语句"列的值
            Object formatStatementObj = row.get("格式化语句");

            if (formatStatementObj == null) {
                continue;
            }

            String formatStatement = formatStatementObj.toString();

            // 判断是否包含BOOKID
            if (formatStatement.contains("BOOKID") || formatStatement.contains("QCJF_WB")) {
                row.put("dp", "DAP研发一部");
                row.put("dp_leader", "王善峰");
                row.put("dp_group", "账务查询项目组");
            }
            // 判断是否包含TM_L
            else if (formatStatement.contains("TM_L") || formatStatement.contains("ACCOU_ACCOU")) {
                row.put("dp", "DAP研发五部");
                row.put("dp_leader", "蔡海");
                row.put("dp_group", "应收应付项目组");
            } else if (formatStatement.contains("TFM_GCM_LEDGER")) {
                row.put("dp", "DAP研发三部");
                row.put("dp_leader", "岳锦华");
                row.put("dp_group", "资金监控项目组");
            } else if (formatStatement.contains("数据集名称") ) {
                row.put("dp", "大数据事业部");
                row.put("dp_leader", "何幼玲");
                row.put("dp_group", "数据平台部/Realinsight产品组");
            }else if (formatStatement.contains("C_")) {
                row.put("dp", "大数据事业部");
                row.put("dp_leader", "何幼玲");
                row.put("dp_group", "数据平台部/Realinsight产品组");
            }
        }
        return finalResult;
    }





    public static List<Map<String, Object>> fillAndRemoveDpGroup(List<Map<String, Object>> finalData) {
        if (finalData == null || finalData.isEmpty()) {
            return finalData;
        }

        for (Map<String, Object> row : finalData) {
            // 获取"归属项目组（调整）"的值
            Object projectGroup = row.get("归属项目组（调整）");

            // 如果为空，则用dp_group填充
            if (projectGroup == null || projectGroup.toString().trim().isEmpty()) {
                Object dpGroup = row.get("dp_group");
                if (dpGroup != null) {
                    row.put("归属项目组（调整）", dpGroup);
                }
            }

            // 删除dp_group列
            row.remove("dp_group");
        }

        return finalData;
    }

    }













