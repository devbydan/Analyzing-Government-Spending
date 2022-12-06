cd /home/$USER/IdeaProjects/Analyzing-Government-Spending/ # go to project dir
mvn compile # maven compile
mvn package # maven package
cd /home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.3.1/bin # homebrew spark install dir
sudo ./spark-submit --class SparkWorks.SparkMainApp ~/IdeaProjects/Analyzing-Government-Spending/target/test-1.8-SNAPSHOT.jar --master spark://$hostname:7077 # instantiate spark with jar file for project
cd ~ # return to home dir
