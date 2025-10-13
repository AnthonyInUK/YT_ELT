#!/bin/bash

# Power BI 数据导出脚本
# 用法: 
#   手动导出: ./export_to_powerbi.sh
#   设置定时: ./export_to_powerbi.sh --setup-cron
#   移除定时: ./export_to_powerbi.sh --remove-cron

# 处理命令行参数
if [ "$1" = "--setup-cron" ]; then
    echo "⚙️  设置定时自动导出..."
    echo ""
    
    # 获取脚本的绝对路径
    SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
    
    # 检查是否已存在定时任务
    if crontab -l 2>/dev/null | grep -q "$SCRIPT_PATH"; then
        echo "⚠️  定时任务已存在！"
        echo ""
        echo "当前设置："
        crontab -l | grep "$SCRIPT_PATH"
        echo ""
        echo "如需重新设置，请先运行: $0 --remove-cron"
        exit 0
    fi
    
    # 添加定时任务（每天凌晨3点运行，在 Airflow DAG 运行之后）
    (crontab -l 2>/dev/null; echo "0 3 * * * $SCRIPT_PATH >> /Users/anthony/Desktop/DataEngineerCourse/powerbi_export.log 2>&1") | crontab -
    
    echo "✅ 定时任务设置成功！"
    echo ""
    echo "📅 计划："
    echo "   • 每天凌晨 3:00 自动导出数据"
    echo "   • 日志文件: /Users/anthony/Desktop/DataEngineerCourse/powerbi_export.log"
    echo ""
    echo "查看当前定时任务："
    crontab -l | grep "$SCRIPT_PATH"
    echo ""
    exit 0
fi

if [ "$1" = "--remove-cron" ]; then
    echo "🗑️  移除定时自动导出..."
    
    # 获取脚本的绝对路径
    SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
    
    # 移除定时任务
    crontab -l 2>/dev/null | grep -v "$SCRIPT_PATH" | crontab -
    
    echo "✅ 定时任务已移除！"
    exit 0
fi

echo "🔄 开始导出 Power BI 数据..."
echo "⏰ $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# 设置输出文件路径
OUTPUT_FILE="/Users/anthony/Desktop/youtube_powerbi_data.csv"

# 从数据库导出数据
docker exec postgres psql -U postgres -d elt_db -c "COPY (
    SELECT 
        \"Video_ID\" as video_id,
        \"Video_Title\" as video_title,
        \"Upload_Date\" as upload_date,
        EXTRACT(YEAR FROM \"Upload_Date\") as upload_year,
        EXTRACT(MONTH FROM \"Upload_Date\") as upload_month,
        TO_CHAR(\"Upload_Date\", 'YYYY-MM') as upload_month_name,
        \"Duration\" as duration,
        EXTRACT(EPOCH FROM \"Duration\"::time) as duration_seconds,
        CASE 
            WHEN EXTRACT(EPOCH FROM \"Duration\"::time) <= 60 THEN 'Shorts'
            ELSE 'Normal'
        END as video_type,
        \"View_Count\" as view_count,
        \"Like_Count\" as like_count,
        \"Comment_Count\" as comment_count,
        ROUND(CAST(\"Like_Count\" AS NUMERIC) / NULLIF(\"View_Count\", 0) * 100, 2) as like_rate,
        ROUND(CAST(\"Comment_Count\" AS NUMERIC) / NULLIF(\"View_Count\", 0) * 100, 2) as comment_rate,
        ROUND(CAST(\"Like_Count\" + \"Comment_Count\" AS NUMERIC) / NULLIF(\"View_Count\", 0) * 100, 2) as engagement_rate
    FROM core.yt_api
    ORDER BY \"Upload_Date\" DESC
) TO STDOUT WITH CSV HEADER;" > "$OUTPUT_FILE"

# 检查是否成功
if [ $? -eq 0 ]; then
    # 获取记录数
    RECORDS=$(tail -n +2 "$OUTPUT_FILE" | wc -l | xargs)
    FILE_SIZE=$(ls -lh "$OUTPUT_FILE" | awk '{print $5}')
    
    echo "✅ 导出成功！"
    echo ""
    echo "📊 数据统计："
    echo "   • 记录数: $RECORDS 条"
    echo "   • 文件大小: $FILE_SIZE"
    echo "   • 文件位置: $OUTPUT_FILE"
    echo ""
    echo "📤 下一步："
    echo "   1. 打开 https://app.powerbi.com"
    echo "   2. 删除旧的数据集"
    echo "   3. 上传新的 CSV 文件"
    echo ""
else
    echo "❌ 导出失败！请检查 Docker 容器是否运行。"
    exit 1
fi

