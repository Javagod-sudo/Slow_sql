package util;

import lombok.Data;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static util.cellUtil.getCellValue;
@Data
public class ExcelUtil {


    /**
     * 读取 Excel 文件的指定 sheet，返回 List<Map<String, Object>>
     * 键为表头（第一行），值为对应单元格内容
     *
     * @param filePath   文件路径
     * @param sheetIndex sheet 索引（从0开始）
     * @return 所有行数据
     * @throws IOException
     */
    public static List<Map<String, Object>> readExcel(String filePath, int sheetIndex) throws IOException {
        List<Map<String, Object>> allData = new ArrayList<>();

        try (FileInputStream fis = new FileInputStream(filePath);
             Workbook workbook = WorkbookFactory.create(fis)){

            Sheet sheet = workbook.getSheetAt(sheetIndex);
            if (sheet == null) {
                throw new IllegalArgumentException("Sheet index " + sheetIndex + " 不存在！");
            }

            // 读取表头（第一行）
            Row headerRow = sheet.getRow(0);
            if (headerRow == null) {
                return allData; // 空表
            }

            List<String> headers = new ArrayList<>();
            for (Cell cell : headerRow) {
                headers.add(cell.getStringCellValue().trim());
            }

            // 从第2行开始读取数据（索引1）
            for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row dataRow = sheet.getRow(rowIndex);
                if (dataRow == null) continue;

                Map<String, Object> rowData = new LinkedHashMap<>(); // 保持列顺序

                for (int i = 0; i < headers.size(); i++) {
                    Cell cell = dataRow.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                    Object cellValue = getCellValue(cell);
                    rowData.put(headers.get(i), cellValue);
                }

                // 关键：一行读完后再加一次！
                allData.add(rowData);
            }
        }

        return allData;
    }










    // 例子：把“分数≥90 且 北京”的名单导出
    public static void exportToExcel(List<Map<String, Object>> list, String outputPath,String sheetname) throws IOException {
        try (Workbook wb = new XSSFWorkbook()) {
            Sheet s = wb.createSheet(sheetname);
            // 写表头
            if (!list.isEmpty()) {
                Row header = s.createRow(0);
                List<String> keys = new ArrayList<>(list.get(0).keySet());
                for (int i = 0; i < keys.size(); i++) {
                    header.createCell(i).setCellValue(keys.get(i));
                }
                // 写数据
                for (int i = 0; i < list.size(); i++) {
                    Row row = s.createRow(i + 1);
                    Map<String, Object> map = list.get(i);
                    for (int j = 0; j < keys.size(); j++) {
                        Object val = map.get(keys.get(j));
                        if (val != null) {
                            row.createCell(j).setCellValue(val.toString());
                        }
                    }
                }
            }
            try (FileOutputStream fos = new FileOutputStream(outputPath)) {
                wb.write(fos);
            }
            System.out.println("导出成功 → " + outputPath);
        }
    }



    /**
     * 从SQL语句中提取表名并展开成多行记录（使用List指定字段）
     *
     * @param dataList 原始数据列表
     * @param idField ID字段名
     * @param sqlField SQL语句字段名
     * @param additionalFields 需要额外带出的字段列表
     * @return 展开后的结果列表
     */
    public static List<Map<String, Object>> extractTableNamesExpandedWithFields(
            List<Map<String, Object>> dataList,
            String idField,
            String sqlField,
            List<String> additionalFields) {

        List<Map<String, Object>> result = new ArrayList<>();

        String regex = "(?i)(?:from|join|update|into|delete\\s+from|create\\s+(?:or\\s+replace\\s+)?(?:table|view|materialized\\s+view)|drop\\s+(?:table|view|materialized\\s+view|index)|truncate\\s+table|alter\\s+table|insert\\s+into)\\s+(?:if\\s+(?:not\\s+)?exists\\s+)?([a-z_][a-z0-9_]*)";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

        for (Map<String, Object> row : dataList) {
            String id = String.valueOf(row.get(idField));
            String sqlText = (String) row.get(sqlField);

            if (sqlText != null && !sqlText.trim().isEmpty()) {
                Matcher matcher = pattern.matcher(sqlText);

                while (matcher.find()) {
                    String tableName = matcher.group(1);
                    if (tableName != null && !tableName.trim().isEmpty()) {
                        tableName = tableName.replace("FMP_QUERY_DB.", "");

                        Map<String, Object> newRow = new LinkedHashMap<>();
                        newRow.put(idField, id);
                        newRow.put("table_name", tableName);

                        // 添加额外的字段
                        if (additionalFields != null && !additionalFields.isEmpty()) {
                            for (String field : additionalFields) {
                                newRow.put(field, row.get(field));
                            }
                        }

                        result.add(newRow);
                    }
                }
            }
        }

        return result;
    }



    public static List<Map<String, Object>> extractTableNamesWithFieldsDistinct(
            List<Map<String, Object>> dataList,
            String sqlField,
            List<String> additionalFields) {

        // 使用 Map 来去重，key 为所有字段值的组合
        Map<String, Map<String, Object>> distinctMap = new LinkedHashMap<>();

        String regex = "(?i)(?:from|join|update|into|delete\\s+from|create\\s+(?:or\\s+replace\\s+)?(?:table|view|materialized\\s+view)|drop\\s+(?:table|view|materialized\\s+view|index)|truncate\\s+table|alter\\s+table|insert\\s+into)\\s+(?:if\\s+(?:not\\s+)?exists\\s+)?([a-z_][a-z0-9_]*)";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

        for (Map<String, Object> row : dataList) {
            // 修改1：安全获取 SQL 字段，处理不同类型
            Object sqlTextObj = row.get(sqlField);
            String sqlText = null;

            if (sqlTextObj instanceof String) {
                sqlText = (String) sqlTextObj;
            } else if (sqlTextObj != null) {
                sqlText = sqlTextObj.toString();
            }

            if (sqlText != null && !sqlText.trim().isEmpty()) {
                Matcher matcher = pattern.matcher(sqlText);

                while (matcher.find()) {
                    String tableName = matcher.group(1);
                    if (tableName != null && !tableName.trim().isEmpty()) {
                        tableName = tableName.replace("FMP_QUERY_DB.", "");

                        Map<String, Object> newRow = new LinkedHashMap<>();
                        newRow.put("table_name", tableName);

                        // 修改2：添加额外的字段（保持原始类型）
                        if (additionalFields != null && !additionalFields.isEmpty()) {
                            for (String field : additionalFields) {
                                newRow.put(field, row.get(field));  // 直接保存，不转换类型
                            }
                        }

                        // 修改3：生成唯一键用于去重（安全处理 null 和不同类型）
                        StringBuilder keyBuilder = new StringBuilder();
                        keyBuilder.append(tableName);
                        if (additionalFields != null) {
                            for (String field : additionalFields) {
                                Object value = row.get(field);
                                // 使用 toString() 并处理 null
                                keyBuilder.append("|").append(value == null ? "null" : value.toString());
                            }
                        }
                        String uniqueKey = keyBuilder.toString();

                        // 去重：只保留第一次出现的
                        distinctMap.putIfAbsent(uniqueKey, newRow);
                    }
                }
            }
        }

        return new ArrayList<>(distinctMap.values());
    }

    /**
     * 合并多个提取结果并按所有字段去重
     *
     * @param resultLists 多个结果列表
     * @return 按所有字段组合去重后的结果
     */
    public static List<Map<String, Object>> mergeAndDistinctAllFields(List<Map<String, Object>>... resultLists) {
        Map<String, Map<String, Object>> distinctMap = new LinkedHashMap<>();

        for (List<Map<String, Object>> resultList : resultLists) {
            if (resultList != null) {
                for (Map<String, Object> row : resultList) {
                    // 生成唯一键：基于所有字段名和值的组合
                    StringBuilder keyBuilder = new StringBuilder();

                    // 按字段名排序，确保顺序一致
                    List<String> sortedKeys = new ArrayList<>(row.keySet());
                    Collections.sort(sortedKeys);

                    for (String key : sortedKeys) {
                        Object value = row.get(key);
                        keyBuilder.append(key)
                                .append("=")
                                .append(value == null ? "null" : value.toString())
                                .append(";");
                    }

                    String uniqueKey = keyBuilder.toString();

                    // 只保留第一次出现的记录
                    distinctMap.putIfAbsent(uniqueKey, new LinkedHashMap<>(row));
                }
            }
        }

        return new ArrayList<>(distinctMap.values());
    }


    public static List<Map<String, Object>> mergeByInternalId(List<Map<String, Object>> source, String idKey) {
        if (source == null || source.isEmpty()) {
            return new ArrayList<>();
        }

        // key = 内部ID，value = 这组的所有行
        Map<Object, List<Map<String, Object>>> groupMap = new LinkedHashMap<>();

        for (Map<String, Object> row : source) {
            Object id = row.get(idKey);
            if (id == null) continue;

            groupMap.computeIfAbsent(id, k -> new ArrayList<>()).add(row);
        }

        // 合并结果
        List<Map<String, Object>> result = new ArrayList<>();

        for (Map.Entry<Object, List<Map<String, Object>>> entry : groupMap.entrySet()) {
            Object id = entry.getKey();
            List<Map<String, Object>> rows = entry.getValue();

            // 用来存放合并后的一行
            Map<String, Object> mergedRow = new LinkedHashMap<>();

            // 先把 ID 放进去
            mergedRow.put(idKey, id);

            // 找出这一组里出现过的所有 key（除了 ID 列）
            Set<String> allKeys = new LinkedHashSet<>();
            for (Map<String, Object> row : rows) {
                allKeys.addAll(row.keySet());
            }
            allKeys.remove(idKey);

            // 每一列单独收集 → 去重 → 用空格拼接
            for (String key : allKeys) {
                Set<String> values = new LinkedHashSet<>();  // LinkedHashSet 保持插入顺序
                for (Map<String, Object> row : rows) {
                    Object val = row.get(key);
                    if (val != null) {
                        String str = val.toString().trim();
                        if (!str.isEmpty()) {
                            values.add(str);
                        }
                    }
                }
                mergedRow.put(key, String.join(",", values));
            }

            result.add(mergedRow);
        }

        return result;
    }

    /**
     * 【新增】支持追加 Sheet 到已存在的 Excel 文件（推荐使用这个）
     * 如果文件不存在会自动创建，如果 sheet 名重复会自动重命名
     */
    public static void exportToExcelAppend(List<Map<String, Object>> list,
                                           String outputPath,
                                           String sheetName) throws IOException {

        java.io.File file = new java.io.File(outputPath);
        Workbook wb = null;
        boolean isNewFile = !file.exists();

        try {
            if (isNewFile) {
                // 文件不存在 → 新建
                wb = new XSSFWorkbook();
            } else {
                // 文件存在 → 读取旧文件
                try (FileInputStream fis = new FileInputStream(file)) {
                    wb = new XSSFWorkbook(fis);
                }
            }

            // 处理重名 sheet（自动加 (1)(2)(3)...）
            String finalSheetName = sheetName;
            int idx = 1;
            while (wb.getSheet(finalSheetName) != null) {
                finalSheetName = sheetName + "(" + idx++ + ")";
            }

            // === 下面这段代码和你原来的 exportToExcel 完全一样 ===
            Sheet s = wb.createSheet(finalSheetName);

            if (!list.isEmpty()) {
                Row header = s.createRow(0);
                List<String> keys = new ArrayList<>(list.get(0).keySet());
                for (int i = 0; i < keys.size(); i++) {
                    header.createCell(i).setCellValue(keys.get(i));
                }

                for (int i = 0; i < list.size(); i++) {
                    Row row = s.createRow(i + 1);
                    Map<String, Object> map = list.get(i);
                    for (int j = 0; j < keys.size(); j++) {
                        Object val = map.get(keys.get(j));
                        if (val != null) {
                            row.createCell(j).setCellValue(val.toString());
                        }
                    }
                }

                // 可选：自动列宽（提升可读性）
                for (int i = 0; i < keys.size(); i++) {
                    s.autoSizeColumn(i);
                }
            }

            // === 写回文件 ===
            try (FileOutputStream fos = new FileOutputStream(outputPath)) {
                wb.write(fos);
            }

            System.out.println("追加导出成功 → " + outputPath +
                    "  [Sheet: " + finalSheetName + "]");

        } finally {
            if (wb != null) {
                try { wb.close(); } catch (Exception ignored) {}
            }
        }
    }
}
