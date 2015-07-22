# SparkMovieRecommendationApp
Movie Recommendations using Spark MLLib

Step 1: First compile the project: Select project 'lab-exercises' -> Run As -> Maven Install

Step 2: use scp to copy the sparkmovierecapp-1.0.jar to the mapr sandbox or cluster

To run the  lab:

/opt/mapr/spark/spark-1.3.1/bin/spark-submit --class SparkMovieRec --master yarn sparkmovierecapp-1.0.jar

To run the code in the spark-shell:
start the spark shell
/opt/mapr/spark/spark-1.3.1/bin/spark-shell
copy paste the code from the file spark-shell-commands.txt
