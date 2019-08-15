#!/bin/sh
timestart=$(date +%s)
currentdir=$(pwd)
outputdir=$currentdir/amitgoeloutput

echo -e "\nCleaning up\n"
rm -f $outputdir/* > /dev/null 2>&1
rmdir $outputdir > /dev/null 2>&1
aws s3 rm s3://amitgoelc2bdesaavnproject/mroutput --recursive

echo -e "\nClean up completed\n"

if [[ -e $currentdir/saavntrendproject.jar ]]
then

	echo -e "\nExecuting MapReduce\n"
	hadoop jar $currentdir/saavntrendproject.jar com.upgrad.saavntrendproject.SaavnProjectDriver s3a://mapreduce-project-bde/part-00000 s3a://amitgoelc2bdesaavnproject/mroutput

else
	echo -e "\nsaavntrendproject.jar should be present in the directory ($currentdir) from which matchsaavntrend.sh is being executed\n"
	exit 1
fi

status=$(echo $?)

if [[ $status -eq 0 ]]
then

	echo -e "\nCreate directory ($outputdir) for further processing\n"
	mkdir -p $outputdir

	echo -e "\nCopy MapReduce output files from s3 to local directory $outputdir\n"
	aws s3 cp s3://amitgoelc2bdesaavnproject/mroutput/ $outputdir/ --recursive --exclude "_SUCCESS"


	echo -e "\nCopy Saavn Treding list from s3 to local directory $outputdir\n"
	aws s3 cp s3://mapreduce-project-bde/trending_data_daily.csv $outputdir/
	saavnlistfile=$outputdir/trending_data_daily.csv

	echo -e "\nStart further processing\n"
	grep "2017-12-25" $saavnlistfile | cut -d"," -f1 | sort > $outputdir/saavn25.txt
	grep "2017-12-26" $saavnlistfile | cut -d"," -f1 | sort > $outputdir/saavn26.txt
	grep "2017-12-27" $saavnlistfile | cut -d"," -f1 | sort > $outputdir/saavn27.txt
	grep "2017-12-28" $saavnlistfile | cut -d"," -f1 | sort > $outputdir/saavn28.txt
	grep "2017-12-29" $saavnlistfile | cut -d"," -f1 | sort > $outputdir/saavn29.txt
	grep "2017-12-30" $saavnlistfile | cut -d"," -f1 | sort > $outputdir/saavn30.txt
	grep "2017-12-31" $saavnlistfile | cut -d"," -f1 | sort > $outputdir/saavn31.txt

	sort -k2 -nr $outputdir/part-r-00000 > $outputdir/new25.txt
	sort -k2 -nr $outputdir/part-r-00001 > $outputdir/new26.txt
	sort -k2 -nr $outputdir/part-r-00002 > $outputdir/new27.txt
	sort -k2 -nr $outputdir/part-r-00003 > $outputdir/new28.txt
	sort -k2 -nr $outputdir/part-r-00004 > $outputdir/new29.txt
	sort -k2 -nr $outputdir/part-r-00005 > $outputdir/new30.txt
	sort -k2 -nr $outputdir/part-r-00006 > $outputdir/new31.txt

	head -100 $outputdir/new25.txt | cut -d":" -f2 | cut -f1 | sort > $outputdir/25.txt
	head -100 $outputdir/new26.txt | cut -d":" -f2 | cut -f1 | sort > $outputdir/26.txt
	head -100 $outputdir/new27.txt | cut -d":" -f2 | cut -f1 | sort > $outputdir/27.txt
	head -100 $outputdir/new28.txt | cut -d":" -f2 | cut -f1 | sort > $outputdir/28.txt
	head -100 $outputdir/new29.txt | cut -d":" -f2 | cut -f1 | sort > $outputdir/29.txt
	head -100 $outputdir/new30.txt | cut -d":" -f2 | cut -f1 | sort > $outputdir/30.txt
	head -100 $outputdir/new31.txt | cut -d":" -f2 | cut -f1 | sort > $outputdir/31.txt

	echo -e "\nProcessing over\n"
	echo -e "\nLets try to find matching songs\n"
	match25=$(comm -12 $outputdir/25.txt $outputdir/saavn25.txt|wc -l)
	match26=$(comm -12 $outputdir/26.txt $outputdir/saavn26.txt|wc -l)
	match27=$(comm -12 $outputdir/27.txt $outputdir/saavn27.txt|wc -l)
	match28=$(comm -12 $outputdir/28.txt $outputdir/saavn28.txt|wc -l)
	match29=$(comm -12 $outputdir/29.txt $outputdir/saavn29.txt|wc -l)
	match30=$(comm -12 $outputdir/30.txt $outputdir/saavn30.txt|wc -l)
	match31=$(comm -12 $outputdir/31.txt $outputdir/saavn31.txt|wc -l)

	echo -e "\nNumber of songs matching for 25-DEC-2017 : $match25"
	echo -e "\nNumber of songs matching for 26-DEC-2017 : $match26"
	echo -e "\nNumber of songs matching for 27-DEC-2017 : $match27"
	echo -e "\nNumber of songs matching for 28-DEC-2017 : $match28"
	echo -e "\nNumber of songs matching for 29-DEC-2017 : $match29"
	echo -e "\nNumber of songs matching for 30-DEC-2017 : $match30"
	echo -e "\nNumber of songs matching for 31-DEC-2017 : $match31"

	rm -f $outputdir/new??.txt

	echo -e "\nRemoving MapReduce output from s3 as not needed there anymore\n"
	aws s3 rm s3://amitgoelc2bdesaavnproject/mroutput --recursive
	
	timeend=$(date +%s)
	timeelapsed=$(expr $timeend - $timestart)
	timeelapsedminutes=$(expr $timeelapsed / 60)
	echo -e "\nTotal time taken approximately (in minutes): $timeelapsedminutes\n"
	echo -e "\nThank you!!!\n"

else
	echo -e "\nThere was some issue with MapReduce program, probably would have failed due to container exit code 137\n"
	echo -e "\nPlease flush your cache and restart matchsaavntrend.sh\n"
fi