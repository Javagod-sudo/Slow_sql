package test;

import util.ExcelUtil;

import java.util.List;
import java.util.Map;

public class test {
    public static void main(String[] args) throws Exception {
         String DICT_FILE = "dim_map.xls";

        List<Map<String, Object>> dictList = ExcelUtil.readExcelFromResources(DICT_FILE, 0);

        System.out.println(dictList);
    }
}
