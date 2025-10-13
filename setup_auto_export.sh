#!/bin/bash

# 自动化 Power BI 数据导出
# 每天自动导出最新数据到桌面

echo "⏰ 设置自动导出任务..."
echo ""

SCRIPT_PATH="/Users/anthony/Desktop/DataEngineerCourse/export_to_powerbi.sh"

# 创建 crontab 任务（每天早上 9 点自动导出）
CRON_JOB="0 9 * * * $SCRIPT_PATH >> /Users/anthony/Desktop/DataEngineerCourse/export.log 2>&1"

# 检查是否已存在
if crontab -l 2>/dev/null | grep -q "$SCRIPT_PATH"; then
    echo "⚠️  自动任务已存在"
    echo ""
    echo "当前计划任务："
    crontab -l | grep "$SCRIPT_PATH"
else
    # 添加到 crontab
    (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
    echo "✅ 自动导出任务已设置！"
    echo ""
    echo "📅 执行计划："
    echo "   • 时间: 每天早上 9:00"
    echo "   • 操作: 自动导出最新数据到桌面"
    echo "   • 日志: /Users/anthony/Desktop/DataEngineerCourse/export.log"
fi

echo ""
echo "💡 提示："
echo "   • 查看任务: crontab -l"
echo "   • 删除任务: crontab -e (然后删除相关行)"
echo "   • 手动运行: $SCRIPT_PATH"

