touch current_log.log
file=current_log.log
maxsize=2048 #2mb

cityArray=("Istanbul" "Tokyo" "Beijing" "London" "Moskow")
logLevelsArray=("INFO" "WARN" "FATAL" "DEBUG" "ERROR")
echo "Logs are being generated. (CTRL+C to exit)"
while :
do
actualsize=$(du -k "$file" | cut -f 1)
if [ $actualsize -le $maxsize ]; then
   # echo size is under $maxsize kilobytes
   rand=$[$RANDOM % 5]
   rand2=$[$RANDOM % 5]
   date_=$(date +%Y-%m-%d-%T)
   printf $date_'\t'${logLevelsArray[$rand2]}'\t'${cityArray[$rand]}'\t''Hello from '${cityArray[$rand]}'\n' >> $file
else
    # echo size is over $maxsize kilobytes
    cat $file > $date_'_log.log'
    > $file
fi
   sleep 0.25
done