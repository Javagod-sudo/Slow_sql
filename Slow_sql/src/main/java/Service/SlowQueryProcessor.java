package Service;

import Extractor.SlowQueryTableExtractor;
import util.ExcelUtil;
import util.TableJoin;

import java.util.*;

/**
 * 慢查询台账一键处理工具类（最终推荐版）
 */
public class SlowQueryProcessor {

    // 固定配置
    private static final String DICT_FILE = "dim_map.xls";
    private static final String TARGET_DEPT = "大数据事业部";
    private static final String FIELD_NAME_DEPT = "微服务负责部门";
    private static final String SQL_FIELD = "原始语句";
    private static final String OUTPUT_DIR = "D:\\远光\\cwzt性能优化\\最终结果\\";


    private static final List<String> DICT_FIELDS     = Arrays.asList("dp", "dp_leader", "dp_group");
    private static final List<String> DICT_FIELDS_02  = Arrays.asList("table_name", "dp", "dp_leader", "dp_group");
    private static final List<String> DICT_FIELDS_03  = Arrays.asList("dp_group");

    /**
     * 一键处理慢查询台账并导出
     *
     * @param filePath     主Excel文件完整路径
     * @param sheetIndex   要读取的工作表索引（从0开始，例如：2）
     * @param sheetName    导出Excel中的工作表名称（如 "大数据慢查询结果"）
     * @param excelName    导出文件名（例如：sheet06.xlsx）
     */
    public static void processSlowQueryExcel(
            String filePath,
            int sheetIndex,
            String sheetName,
            String excelName) {

        try {
            // 1. 读取维表
           // List<Map<String, Object>> dictList = ExcelUtil.readExcel(DICT_FILE, 0);
            List<Map<String, Object>> dictList = ExcelUtil.readExcelFromResources(DICT_FILE, 0);
            // 2. 读取主表全量数据（用于最后关联 dp_group）
            List<Map<String, Object>> dataList = ExcelUtil.readExcelFromResources(filePath, sheetIndex);

            // 3. 过滤大数据事业部
            List<Map<String, Object>> bigDataDeptList = SlowQueryTableExtractor.filterTable(
                   // ExcelUtil.readExcel(filePath, sheetIndex),
                    ExcelUtil.readExcelFromResources(filePath, sheetIndex),
                    FIELD_NAME_DEPT,
                    TARGET_DEPT
            );

            // 4. 解析 SQL 中的表名
            List<Map<String, Object>> tableNameExtracted = SlowQueryTableExtractor.extractTableNamesExpanded(
                    bigDataDeptList, "内部ID", SQL_FIELD);

            // 5. 表名关联维表
            List<Map<String, Object>> joinedWithDict = TableJoin.leftJoin(
                    tableNameExtracted, dictList,
                    "table_name", "table_name",
                    DICT_FIELDS, true);

            // 6. 按内部ID合并回大数据事业部行
            List<Map<String, Object>> mergedById = ExcelUtil.mergeByInternalId(joinedWithDict, "内部ID");

            List<Map<String, Object>> finalResult = TableJoin.leftJoin(
                    bigDataDeptList, mergedById,
                    "内部ID", "内部ID",
                    DICT_FIELDS_02, true);

            // 7. 后续处理（去重、排序等）
            List<Map<String, Object>> finalResult02 = SlowQueryTableExtractor.processFinalResult(finalResult);

            // 8. 将 dp_group 关联回全量数据
            List<Map<String, Object>> finalData = TableJoin.leftJoin(
                    dataList, finalResult02,
                    "内部ID", "内部ID",
                    DICT_FIELDS_03, true);

            // 9. 填充空值 + 清理字段
            List<Map<String, Object>> exportData = SlowQueryTableExtractor.fillAndRemoveDpGroup(finalData);

            // 10. 导出（使用你指定的 sheetName）
            String outputPath = OUTPUT_DIR + excelName;
            ExcelUtil.exportToExcelAppend(exportData, outputPath, sheetName);

            System.out.println("处理完成！文件已成功导出：");
            System.out.println("路径：" + outputPath);
            System.out.println("工作表名：" + sheetName);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("慢查询处理失败：" + e.getMessage(), e);
        }
    }



}