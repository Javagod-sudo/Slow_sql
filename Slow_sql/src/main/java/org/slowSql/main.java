package org.slowSql;


import Extractor.SlowQueryTableExtractor;
import util.ExcelUtil;
import util.TableJoin;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static Service.SlowQueryProcessor.processSlowQueryExcel;

public class main {



    public static void main(String[] args)  {


        processSlowQueryExcel(
                "D:\\远光\\cwzt性能优化\\数据字典_提质增效场景 20250709.xlsx",
                1,                                      // 第2个sheet
                "监控中心1级",                    // 导出后sheet名称
                "cwzt慢查询分析.xlsx"                          // 导出文件名
        );
        processSlowQueryExcel(
                "D:\\远光\\cwzt性能优化\\数据字典_提质增效场景 20250709.xlsx",
                2,                                      // 第3个sheet
                "监控中心2级",                    // 导出后sheet名称
                "cwzt慢查询分析.xlsx"                          // 导出文件名
        );
        processSlowQueryExcel(
                "D:\\远光\\cwzt性能优化\\数据字典_提质增效场景 20250709.xlsx",
                3,                                      // 第4个sheet
                "日常日志审查",                    // 导出后sheet名称
                "cwzt慢查询分析.xlsx"                          // 导出文件名
        );


    }
}