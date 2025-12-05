package Slow_sql;

import Slow_sql.util.cellUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class main {

    public static void main(String[] args) throws IOException {
        // 1. 你的真实文件路径（原样复制即可）
        String filePath = "D:\\远光\\慢查询梳理\\20251201_score.xlsx";

        // 用来存放最终读取出来的所有数据
        List<Map<String, Object>> alldata = new ArrayList<>();

        // 2. 打开 Excel 文件
        FileInputStream fis = new FileInputStream(filePath);

        Workbook workbook = new XSSFWorkbook(fis);


        // 3. 读取第三个 Sheet（从0开始数，第3个就是 index=2）
        Sheet sheet = workbook.getSheetAt(15);

        Row headerRow = sheet.getRow(0);

        List<String> cellList = new ArrayList<>();
        for (Cell cell : headerRow) {
            cellList.add(cell.getStringCellValue().trim());
        }


// 从第二行开始读取数据（索引为1）
        for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
            Row dataRow = sheet.getRow(rowIndex);
            if (dataRow == null) continue;
// 每一行数据用一个 Map 保存，保持列的顺序
            Map<String, Object> rowData = new LinkedHashMap<>();

            for(int i = 0; i < cellList.size(); i++) {
                Cell cell = dataRow.getCell(i);

                Object cellValue = cellUtil.getCellValue(cell); // 统一处理各种单元格类型
                String columnName = cellList.get(i);

                rowData.put(columnName, cellValue);
               // 当前行所有列都读完了，加入总列表
                alldata.add(rowData);



            }
        }

      for (int i=0;i<1;i++){

          for (int j=0;j<cellList.size();j++)
          {
              String colname = cellList.get(j);
              System.out.println(colname+':'+alldata.get(i).get(colname));


          }

      }
    }
}