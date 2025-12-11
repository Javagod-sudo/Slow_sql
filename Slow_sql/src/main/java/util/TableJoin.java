package util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TableJoin {

    /**
     * 通用的列表关联方法（LEFT JOIN）
     *
     * @param leftList 左表数据列表
     * @param rightList 右表数据列表
     * @param leftJoinKey 左表关联字段名
     * @param rightJoinKey 右表关联字段名
     * @param selectFields 需要从右表选取的字段列表，如果为null则选取所有字段
     * @param ignoreCase 是否忽略大小写进行匹配
     * @return 关联后的结果列表
     */
    public static List<Map<String, Object>> leftJoin(
            List<Map<String, Object>> leftList,
            List<Map<String, Object>> rightList,
            String leftJoinKey,
            String rightJoinKey,
            List<String> selectFields,
            boolean ignoreCase) {

        // 构建右表映射，key为关联字段值，value为右表行数据
        Map<String, Map<String, Object>> rightMap = new LinkedHashMap<>();
        for (Map<String, Object> rightRow : rightList) {
            Object keyValue = rightRow.get(rightJoinKey);
            if (keyValue != null) {
                String key = ignoreCase ? keyValue.toString().toLowerCase() : keyValue.toString();
                if (!key.trim().isEmpty()) {
                    rightMap.put(key, rightRow);
                }
            }
        }

        // 执行关联
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> leftRow : leftList) {
            Map<String, Object> newRow = new LinkedHashMap<>(leftRow);

            Object keyValue = leftRow.get(leftJoinKey);
            if (keyValue != null) {
                String key = ignoreCase ? keyValue.toString().toLowerCase() : keyValue.toString();
                Map<String, Object> rightRow = rightMap.get(key);

                if (rightRow != null) {
                    // 如果指定了选取字段，只添加这些字段
                    if (selectFields != null && !selectFields.isEmpty()) {
                        for (String field : selectFields) {
                            newRow.put(field, rightRow.get(field));
                        }
                    } else {
                        // 否则添加右表所有字段（排除关联键以避免重复）
                        for (Map.Entry<String, Object> entry : rightRow.entrySet()) {
                            if (!entry.getKey().equals(rightJoinKey)) {
                                newRow.put(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                } else {
                    // 如果没有匹配，填充null
                    if (selectFields != null && !selectFields.isEmpty()) {
                        for (String field : selectFields) {
                            newRow.put(field, null);
                        }
                    }
                }
            }

            result.add(newRow);
        }

        return result;
    }

}
