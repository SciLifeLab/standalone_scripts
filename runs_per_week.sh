function runs_per_week()
{
    local fc_root_folders=/proj/ngi2016003/*/
    local week=$1 year=$2
    local week_num_of_Jan_1 week_day_of_Jan_1
    local first_Mon
    local date_fmt="+%y %m %d"
    local date_ftm_year="+%y"
    local date_fmt_month="+%m"
    local date_fmt_day="+%d"

    week_num_of_Jan_1=$(date -d $year-01-01 +%W)
    week_day_of_Jan_1=$(date -d $year-01-01 +%u)

    if ((week_num_of_Jan_1)); then
        first_Mon=$year-01-01
    else
        first_Mon=$year-01-$((01 + (7 - week_day_of_Jan_1 + 1) ))
    fi


    YEAR=${year: -2}
    MONTH_START=$(date -d "$first_Mon +$((week - 1)) week" "$date_fmt_month")
    MONTH_END=$(date -d "$first_Mon +$((week - 1)) week + 6 day" "$date_fmt_month")
    DAY_START=$(date -d "$first_Mon +$((week - 1)) week" "$date_fmt_day")
    DAY_END=$(date -d "$first_Mon +$((week - 1)) week + 6 day" "$date_fmt_day")

    mon=$(date -d "$first_Mon +$((week - 1)) week" "$date_fmt")
    sun=$(date -d "$first_Mon +$((week - 1)) week + 6 day" "$date_fmt")
    DAYS=()
    if [ $MONTH_START -ne $MONTH_END ];
    then
        DAYS=($(seq -f "$YEAR$MONTH_START%02g" $DAY_START 1 31) $(seq -f "$YEAR$MONTH_END%02g" 1 1 $DAY_END))
    else
        DAYS=($(seq -f "$YEAR$MONTH_START%02g" $DAY_START 1 $DAY_END))
    fi

    RUNS_PER_WEEK_X=0
    RUNS_PER_WEEK_nonX=0
    for DAY in "${DAYS[@]}" ; do
        DAY_RUNS_X=`ls -d $fc_root_folders/$DAY*_ST* 2> null | wc -l`
        RUNS_PER_WEEK_X=`expr $DAY_RUNS_X + $RUNS_PER_WEEK_X`
        DAY_RUNS_nonX=`ls -d $fc_root_folders/$DAY* 2> null | grep -v ST | grep -v 000000  | wc -l`
        RUNS_PER_WEEK_nonX=`expr $DAY_RUNS_nonX + $RUNS_PER_WEEK_nonX`
    done
    echo $week $RUNS_PER_WEEK_X $RUNS_PER_WEEK_nonX

}

CURRENT_YEAR=`date +"%Y"`
CURRENT_WEEK=`date +"%V"`
for WEEK in `seq 1 1 $CURRENT_WEEK`; do
        runs_per_week $WEEK $CURRENT_YEAR
done
