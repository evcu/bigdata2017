
SPARKCODE=$(echo "$2/task$1".py)
DIFFFILE="results/task$1.diff"
TMPFILE="$2/task$1tmp.out"
if [ -e "$DIFFFILE" ]; then
	rm "$DIFFFILE" 
fi

/usr/bin/hadoop fs -rm -r "task$1.out"

if [ "$1" -eq 1 ]
then
	spark-submit "$SPARKCODE" /user/ecc290/HW1data/parking-violations.csv /user/ecc290/HW1data/open-violations.csv
	/usr/bin/hadoop fs -getmerge "task$1.out" "$TMPFILE".tmp
	cat "$TMPFILE".tmp | sort -n > "$TMPFILE"
	rm "$TMPFILE".tmp
elif [ "$1" -eq 6 ]
then
	spark-submit "$SPARKCODE" /user/ecc290/HW1data/parking-violations.csv
	/usr/bin/hadoop fs -getmerge "task$1.out" "$TMPFILE.2"
	cat "$TMPFILE.2" | sort -n > "$TMPFILE"
elif [ "$1" -eq 3 ]
then
	spark-submit "$SPARKCODE" /user/ecc290/HW1data/open-violations.csv
	/usr/bin/hadoop fs -getmerge "task$1.out" "$TMPFILE".tmp
	cat "$TMPFILE".tmp | sort -n > "$TMPFILE"
	rm "$TMPFILE".tmp
	
else
	spark-submit "$SPARKCODE" /user/ecc290/HW1data/parking-violations.csv
	/usr/bin/hadoop fs -getmerge "task$1.out" "$TMPFILE".tmp
	cat "$TMPFILE".tmp | sort -n > "$TMPFILE"
	rm "$TMPFILE".tmp
	
fi

if [ -e "$TMPFILE" ]
then
	DIFF=$(diff -w "keys/task$1.out" "$TMPFILE")
	DIFFC=$(diff -w -y "keys/task$1.out" "$TMPFILE")
	if [ "$DIFF" ]
	then 
		if [ ! -d "results" ]
		then
			mkdir results
		fi
		echo "$DIFFC" > "results/task$1.diff"
		echo "Task $1: Failed. See errors in results/task$1.diff"
	else
	        if [ "$1" -eq 6 ]
                then
                        cat "$TMPFILE.2" | python task6_checksort.py
                        if [ "$?" -eq 1 ]
                        then
                                echo "Task $1: Passed."
                        else
                                echo "Task $1: Failed. Output not sorted."
                        fi
                else
                        echo "Task $1: Passed."
                fi

	fi
else
	echo "Task $1: Failed; no output generated."
fi
rm -f "$TMPFILE"  


