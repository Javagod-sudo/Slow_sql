package org.slowSql;


import Extractor.SlowQueryTableExtractor;
import util.ExcelUtil;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class main {



    public static void main(String[] args) throws IOException {
        // 1. 你的真实文件路径（原样复制即可）
        String filePath = "C:\\yg项目\\慢查询\\20251201_score.xlsx";
        String dictFile = "C:\\yg项目\\慢查询\\dim_tz_map.xls";
        String targetDept = "大数据事业部";
        String fieldName = "所属部门";
        String sqlField="语句";

        List<Map<String, Object>> bigDataDeptList=SlowQueryTableExtractor.filterTable(ExcelUtil.readExcel(filePath, 15),fieldName,targetDept);
        List<Map<String, Object>> dictList = ExcelUtil.readExcel(dictFile, 0); // 你之前那个读取方法








// 2. 转成 Map：英文表名 → 中文名（查找速度飞快！）
        Map<String, String> tableNameMap = new HashMap<>();
        dictList.forEach(row -> {
            String en = String.valueOf(row.get("table_name")).trim().toUpperCase();
            String cn = String.valueOf(row.get("table_comment")).trim();
            if (!en.isEmpty() && !cn.isEmpty() && !"NULL".equalsIgnoreCase(en)) {
                tableNameMap.put(en, cn);
            }
        });




        bigDataDeptList.forEach(row -> {
            String sql = String.valueOf(row.get(sqlField)).trim();
            String enTables = (sql == null || sql.trim().isEmpty())
                    ? ""
                    : SlowQueryTableExtractor.extractTableNames(sql.trim());

            // 英文表名转中文（多个用逗号分隔）
            String cnTables = Arrays.stream(enTables.split(","))
                    .filter(t -> !t.isEmpty())
                    .map(t -> tableNameMap.getOrDefault(t.trim().toUpperCase(), t + "(未知表)"))
                    .collect(Collectors.joining(","));

            // 加到原行里
            row.put("涉及表名", enTables);           // 英文（保留）
            row.put("涉及表中文名", cnTables);        // 新增：中文名
            row.put("表数量", enTables.isEmpty() ? 0 : enTables.split(",").length);
        });

        System.out.println(bigDataDeptList.stream().map(row->(

                        String.valueOf(row.get("涉及表中文名"))
                )).collect(Collectors.toList()));






    }
}