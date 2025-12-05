package Slow_sql.util;

import org.apache.poi.ss.usermodel.Cell;

public class cellUtil {


    public static Object getCellValue(Cell cell) {
    if (cell == null) {
        return "";
    }

    switch (cell.getCellType()) {
        case STRING:
            return cell.getStringCellValue();
        case NUMERIC:
            if (org.apache.poi.ss.usermodel.DateUtil.isCellDateFormatted(cell)) {
                return cell.getDateCellValue();
            } else {
                return cell.getNumericCellValue();
            }
        case BOOLEAN:
            return cell.getBooleanCellValue();
        case FORMULA:
            return cell.getCellFormula();
        default:
            return "";
    }
}

}
