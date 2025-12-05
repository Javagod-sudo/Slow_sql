package util;

import lombok.Data;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

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










}
