cd ~/hadoop-3.3.4/sbin # hadoop dir
./stop-dfs.sh # stops dfs
cd /home/linuxbrew/.linuxbrew/Cellar/apache-spark/3.3.1/libexec/sbin # homebrew install dir
sudo ./stop-master.sh # stops master and worker nodes
sudo ./stop-worker.sh # used to be ./stop-slave.sh but now deprecated
cd ~/hadoop-3.3.4/sbin # go back to hadoop dir
./stop-all.sh # ensure all services are stopped
cd ~ # return to home dir
