
rm -r -f results
mkdir results
rm -f testresults.txt

./testtask.sh 1 $1 | tee -a testresults.txt 
./testtask.sh 2 $1 | tee -a testresults.txt 
./testtask.sh 3 $1 | tee -a testresults.txt 
./testtask.sh 4 $1 | tee -a testresults.txt 
./testtask.sh 5 $1 | tee -a testresults.txt 
./testtask.sh 6 $1 | tee -a testresults.txt 
./testtask.sh 7 $1 | tee -a testresults.txt 

ndiff=$(ls -1 results | wc -l)
if [ "$ndiff" -eq 0 ]
then
	rm -r results
fi


