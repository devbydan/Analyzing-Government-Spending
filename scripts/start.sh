cd ~/hadoop-3.3.4/sbin # navigate to hadoop dir
./start-dfs.sh # start dfs
jps # list the jobs currently running
cd /home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.3.1/libexec/sbin # homebrew spark installation dir
sudo ./start-master.sh # force start master node
sudo ./start-worker.sh spark://$hostname:7077 # force start worker node given path
cd ~ # return to home dir
