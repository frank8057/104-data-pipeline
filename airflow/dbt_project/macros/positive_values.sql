{% test positive_values(model, column_name) %}
-- 測試該欄位的值是否為正數，並且不為空值

SELECT
    {{ column_name }} AS positive_value
FROM {{ model }}
WHERE {{ column_name }} <= 0  -- 檢查小於等於 0 的值
   OR {{ column_name }} IS NULL  -- 檢查空值

{% endtest %}
