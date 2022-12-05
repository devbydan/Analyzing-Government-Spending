<h1 align="center">Welcome to our Big-Data application: Analyzing Spending of Government Funds 👋</h1>
<p>
</p>



> This project of Analyzing Spending of Government Funds is a web-based application using Apache Spark. This application is used to check up-to-date, US government spending data with respect to location. We will use the data provided by the US federal government through [USAspending.gov](https://www.USAspending.gov), an official government website which aggregates information on spending and makes it publicly accessible. The data set contains the amount of funds provided, their recipients, and other relevant identifiers for the transaction. We would also be using supplemental data sets in our search for correlation, determined as we progress through the project. The dataset can be found [here](https://www.usaspending.gov/search/?hash=2d95cc11cb5de7088fe3e4ac503bfdcc).
>
> 

## Author

👤 **Dan Murphy**

* Website: [LinkedIn](https://www.linkedin.com/in/devbydan/)
* GitHub: [@devbydan](https://github.com/devbydan)


## Architecture & Implementation

This application uses Spark stand-alone where Spark occupies the place on top of the Hadoop Distributed File System (HDFS) and space is explicitly allocated for the HDFS. Spark encompasses Hadoop and MapReduce to cover all Spark jobs on the pseudo-cluster instance. The HDFS currently holds a directory of one .csv file which holds US [federal government] spending data but this will be scaled at a later date. All files that are held within the HDFS are split into blocks where each block is replicated *n* amount of times across the pseudo-cluster instance. The NameNode is the master process which keeps track of all of the files within said cluster and the DataNode holds the data.

In respect to this project, Hadoop encompasses two main components: the Hadoop Distributed File System (HDFS) and MapReduce, which is an algorithm that processes large amounts of data efficiently.

To further the scalability of this project, Apache Spark was implemented which processes all database queries on the HDFS as DataFrames with comparatively greater performance than stand-alone Hadoop. DataFrames, a newer abstraction Spark implemented, were used to remedy performance issues of SQL’s union commands. Specifically in terms of the quarterly report functionality, DataFrames were used to speed up the process of returning the large amount of data requested into a user-friendly, organized list of data columns -- which is similar to relational SQL data storage. With performance in mind, Spark is used on top of HDFS for the MapReduce concepts, and is more efficient in running queries on over  of rows 50,000 rows of data for the global and US data sets. 

The project is compiled and packaged with Maven, which is a tool to wrap Java code into a .jar file. This is triggered via the *spark-submit* script where the master node receives the .jar file and sends the job to the slave node which then handles the preprocessed data. Once the Java code is compiled and packaged into the jar file and *spark-submit* is executed, the main menu is displayed in the console. With both datasets in our HDFS, Spark is set up to run queries based on what the user chooses in the main menu, USA menu or Global menu respectively. The data is pre-processed once a dataset is elected. Once a query is selected to run, Spark runs the query on the pre-processed data and returns the desired result. 

## Technologies

- Hadoop 3.2.1
- Apache Spark 3.3.1
- Open JDK, Java 19.0.1
- IntelliJ

## Requirements

```bash
# Install homebrew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
```

```bash
# Mac Xcode development/commandline tools
xcode-select –install
# NOTE: if you are running MacOS Big Sur or newer, run the 2 following commands.
sudo rm -rf /Library/Developer/CommandLineTools # Removes old cmd tools
sudo xcode-select --install # Installs updated tools for new MacOS release
```

``` bash
# Installing prerequisites on Ubuntu
sudo apt install openjdk-8-jdk -y
sudo apt install openssh-server openssh-client -y
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ssh localhost
wget https://downloads.apache.org/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz
tar xzf hadoop-3.2.1.tar.gz
which javac # provides the path to the Java binary dir
readlink -f /usr/bin/javac # linked and assigned to $JAVA_HOME
sudo apt install maven
```

```bash
# Installing prerequisites on Mac
brew install openjdk@11
brew install java
brew install scala
brew install apache-spark
brew install hadoop
```

## Configuration

### Ubuntu:

[Installing/Configuring Hadoop & Spark on Ubuntu](https://dev.to/awwsmm/installing-and-running-hadoop-and-spark-on-ubuntu-18-393h)

### MacOS: 

[Installing/Configuring Hadoop on a Mac](https://towardsdatascience.com/installing-hadoop-on-a-mac-ec01c67b003c)

```bash
# Environment Variables for Hadoop & Spark
# Add the following environment variables to your .bash_profile or .zshrc
# ----------------------------------------------------
# Note: If not sure how to do this, run the following command for either your .bash_profile or .zshrc
sudo nano .bashrc # or -> sudo nano .zshrc
# ----------------------------------------------------
# >>> .bashrc file below <<<
#Hadoop Related Options
export HADOOP_HOME=/home/$USER/hadoop-3.2.1
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

#Apache Spark
export SPARK_HOME=/home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.3.1/libexec
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# >>> end of .bashrc file <<<
# ----------------------------------------------------
# Then paste the environment variables above into either your .bash_profile or .zshrc
# Please ensure you run the following command to apply your changes
source ~/.bashrc # or -> source ~/.zshrc
```

```bash
# Grant binaries executable permissions
chmod +x /home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.3.1/libexec/bin/*
```

```bash
# Verification that spark is installed correctly
spark-shell # runs an instance of spark using scala
```

## Testing

You can simply use the ```start.sh``` , ```run.sh``` and ```stop.sh``` scripts in the root of the repo to start/run the project and stop the services once you are done. If you are to do this, please ensure the hdfs and apache-spark paths are correct.

``` bash
source start.sh # starts Spark services
source run.sh # compiles & packages project using maven, executes the project -> terminal menu
source stop.sh # stops all services
```

**Note: In the ``` start.sh``` script, if you are experiencing that Apache Spark cannot start it's workers, you may need to manually modify the script by replacing ```$hostname``` with the name of your computer.** 

If you would like a hands-on experience, you can follow the instructions below to start the services manually.

1. Start HDFS manually

``` bash
$ hdfs namenode -format -force #for initial setup only
$ cd ~/hadoop-3.2.1/sbin #ubuntu
or
$ cd /usr/local/Cellar/hadoop/3.3.0/sbin #mac
$ ./start-dfs.sh
$ jps # verify that the datanodes and namenodes were started
		Example Output:
		2705 Jps
		2246 NameNode
		2540 SecondaryNameNode
		2381 DataNode
```

2. Create an HDFS directory and put the .csv files in that directory. The .csv files are [here](##Sources) or scroll to the bottom of the README

   ``` bash
   hdfs dfs -mkdir /US-Spending
   hdfs dfs -put ~/Downloads/award.csv /US-Spending
   ```

3. Start Spark manually

```bash
$ cd /home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.3.1/libexec/sbin #ubuntu
or
$ cd /usr/local/Cellar/apache-spark/3.3.1/libexec/sbin #mac
$ ./start-master.sh #spark://$hostname:7077 
$ ./start-slave.sh spark://$hostname:7077 #master is taken as an argument
```

4. Open the project in IntelliJ. 

   1. Ensure the .txt files in the root of the repo are in the /root dir

   **Note:** If you have issues compiling the project, open your terminal and navigate to the root project directory and compile it using maven.

   ``` bash
   $ cd ~/IdeaProjects/Analyzing-Government-Spending # or ~/Documents/Github/.. if cloned there
   $ mvn compile
   $ mvn package
   ```

   2. File > Project Structure > Artifacts > navigate to the directory where the jar is located in the project dir

   ``` bash
   #For example:
   $ cd /home/your_username_here/IdeaProjects/Analyzing-Government-Spending/out/artifacts/<jar_file_here>
   ```

   3. File > Project Structure > Libraries > ..

   4. Classes

   ``` bash
   /home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.3.1/libexec/jars #ubuntu
   or
   /usr/local/Cellar/apache-spark/3.3.1/libexec/jars #mac
   ```

   5. Sources

   ``` bash
   /home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.0.1/libexec/jars #ubuntu
   or
   /usr/local/Cellar/apache-spark/3.3.1/libexec/jars #mac
   ```

Note: this should also show the JAR path you will use in the build window.

5. Run the spark-submit script to run the project

``` bash
$ cd /usr/local/Cellar/apache-spark/3.3.1/bin #navigate to Spark's bin dir
$ ./spark-submit --class <Project Package Name>.SparkMainApp <JAR File of the project> --master <Spark URL you used to start the slave> 

Example input for reference:
$ ./spark-submit --class Analyzing-Government-Spending.SparkMainApp /home/user/IdeaProjects/Analyzing-Government-Spending/target/test-1.8-SNAPSHOT.jar --master Spark://user:7077
```

## Functionality

- [x] Number of Awards per Entity
- [x] Total Award per Recipient Group
- [ ] Total Award Amount by Date Range
- [ ] Total Award per Quarter
- [ ] Top 'K' Awards by Entity
- [ ] List Quarterly Reports by Award Amount

