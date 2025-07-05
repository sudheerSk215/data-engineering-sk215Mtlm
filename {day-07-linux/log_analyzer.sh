!/bin/bash
# Day 7: Linux Command Line
# Production log analyzer script
LOG_FILE=${1:-"access.log"}
REPORT_FILE="log_analysis_$(date+%Y%m%d_%H%M%S).txt"
echo "== Log Analysis Report ===" > $REPORT_FILE
echo "Generated: $(date)" >> $REPORT_FILE
echo "Log file: $LOG_FILE" >> $REPORT_FILE
echo >> $REPORT_FILE
if [! -f "$LOG_FILE"]; then
echo "Error: Log file $LOG_FILE not found"
exit 1
fi 
# Basic Statistics
echo "=== Basic Statistics ===" >> $REPORT_FILE
echo "Total lines: $(wc -l < $LOG_FILE)" >> $REPORT_FILE
echo "File size": $(ls -lh $LOG_FILE | awk '{print $5}')" >> $REPORT_FILE
echo >> $REPORT_FILE
#Error analysis
echo "=== Error Analysis ===" >> $REPORT_FILE
grep -c "ERROR" $LOG_FILE >> $REPORT_FILE
echo >> $REPORT_FILE
echo "Analysis complete : $REPORT_FILE"
