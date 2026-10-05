# Cluster Setup

How we built the cluster the pipeline runs on. Three Ubuntu 22.04 VMs on Google Cloud (free trial credit), one master and two workers.

| Node | Runs | Size |
| --- | --- | --- |
| master | HDFS NameNode, Spark master, Spark history server, ClickHouse, spark-submit | e2-standard-2 (2 vCPU, 8 GB), 100 GB disk |
| worker1 | HDFS DataNode, Spark worker | same |
| worker2 | HDFS DataNode, Spark worker | same |

Software: OpenJDK 17, Hadoop 3.3.6, Spark 3.5.1, ClickHouse 26, Python 3.10. HDFS replication is 2, so every block is on both workers.

GCP resolves the instance names (master, worker1, worker2) on the internal network, so no hosts file editing was needed.

## 1. VMs

```
gcloud config set project csc7740-hdd
gcloud config set compute/zone us-central1-a
for name in master worker1 worker2; do
  gcloud compute instances create $name --machine-type=e2-standard-2 \
    --image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud \
    --boot-disk-size=100GB --boot-disk-type=pd-balanced
done
```

Connect with `gcloud compute ssh master` (same for the workers).

## 2. Every VM

Packages, then a `hadoop` user that owns everything from here on:

```
sudo apt-get update && sudo apt-get install -y openjdk-17-jdk-headless python3-pip python3-venv python3-numpy python3-pandas unzip curl
sudo adduser --disabled-password --gecos "" hadoop
sudo usermod -aG sudo hadoop
echo "hadoop ALL=(ALL) NOPASSWD:ALL" | sudo tee /etc/sudoers.d/hadoop
sudo su - hadoop
```

Hadoop and Spark downloads (Spark from archive.apache.org is slow, about an hour; the dlcdn link for the current 3.5.x is seconds and works the same):

```
curl -LO https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar xzf hadoop-3.3.6.tar.gz && rm hadoop-3.3.6.tar.gz && mv hadoop-3.3.6 hadoop
curl -LO https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar xzf spark-3.5.1-bin-hadoop3.tgz && rm spark-3.5.1-bin-hadoop3.tgz && mv spark-3.5.1-bin-hadoop3 spark
sudo mkdir -p /data/hdfs/namenode /data/hdfs/datanode && sudo chown -R hadoop:hadoop /data
```

Add to `~/.bashrc`:

```
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=$HOME/spark
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3
```

## 3. SSH between nodes

Hadoop and Spark start the worker daemons over SSH from the master. Generate a key on the master (`ssh-keygen -t ed25519`) and append the public key to `~/.ssh/authorized_keys` on all three nodes, master included.

## 4. HDFS

Config files live in `~/hadoop/etc/hadoop/` and are identical on every node (write them on the master and `scp` them over).

`hadoop-env.sh`: add `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`.

`core-site.xml`: `fs.defaultFS` = `hdfs://master:9000`.

`hdfs-site.xml`: `dfs.replication` = 2, `dfs.namenode.name.dir` = `file:///data/hdfs/namenode`, `dfs.datanode.data.dir` = `file:///data/hdfs/datanode`, `dfs.namenode.rpc-bind-host` = `0.0.0.0`.

`workers`: two lines, `worker1` and `worker2`.

Then on the master:

```
hdfs namenode -format -force
start-dfs.sh
hdfs dfsadmin -report | grep "Live datanodes"    # expect (2)
hdfs dfs -mkdir -p /backblaze/raw /spark-logs
```

## 5. Spark

Config in `~/spark/conf/`, again identical everywhere.

`spark-env.sh`:

```
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_CONF_DIR=$HOME/hadoop/etc/hadoop
export SPARK_MASTER_HOST=master
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=6g
export PYSPARK_PYTHON=python3
```

`spark-defaults.conf`:

```
spark.master                     spark://master:7077
spark.executor.memory            5g
spark.executor.cores             2
spark.driver.memory              2g
spark.sql.shuffle.partitions     16
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://master:9000/spark-logs
spark.history.fs.logDirectory    hdfs://master:9000/spark-logs
```

`workers`: `worker1`, `worker2`.

```
start-master.sh && start-workers.sh && start-history-server.sh
spark-submit $SPARK_HOME/examples/src/main/python/pi.py 100 2>&1 | grep "Pi is"
```

## 6. ClickHouse (master only)

```
curl https://clickhouse.com/ | sh
sudo ./clickhouse install
```

Two config changes before starting. It has to listen on the internal network so the Spark executors on the workers can write to it, and its native port has to move because HDFS already owns 9000:

```
sudo tee /etc/clickhouse-server/config.d/listen.xml > /dev/null <<'XML'
<clickhouse><listen_host>0.0.0.0</listen_host></clickhouse>
XML
sudo tee /etc/clickhouse-server/config.d/ports.xml > /dev/null <<'XML'
<clickhouse><tcp_port>9001</tcp_port></clickhouse>
XML
mkdir -p ~/.clickhouse-client && echo '<config><port>9001</port></config>' > ~/.clickhouse-client/config.xml
sudo clickhouse start
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS backblaze"
```

Spark writes through the HTTP port (8123), which is unchanged. Spark needs the shaded JDBC driver, which `--packages` cannot fetch, so download it once:

```
mkdir -p ~/jars && curl -L -o ~/jars/clickhouse-jdbc-0.6.3-all.jar \
  https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.3/clickhouse-jdbc-0.6.3-all.jar
```

## 7. Repo, environment, data

```
git clone https://github.com/alexisharveyyy/CSC7740-Project.git ~/CSC7740-Project
cd ~/CSC7740-Project && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
```

Add to `~/.bashrc` (see `.env.example`):

```
export BACKBLAZE_HDFS_ROOT=hdfs://master:9000/backblaze
export CLICKHOUSE_JDBC_URL=jdbc:clickhouse://master:8123/backblaze?compress=0
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=
export CLICKHOUSE_JDBC_JAR=$HOME/jars/clickhouse-jdbc-0.6.3-all.jar
```

`compress=0` is required. ClickHouse 26 changed its default HTTP compression and without it the JDBC driver fails on Spark's table-existence check with `Invalid LZ4 magic byte`, which Spark misreads as the table not existing.

Load a quarter (about 1.2 GB zipped, 11 GB of CSV):

```
scripts/download_backblaze.sh "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2026.zip" data_Q1_2026
scripts/load_raw_to_hdfs.sh data_Q1_2026
```

## 8. Running

All stages in order:

```
scripts/run_pipeline.sh
```

Or one at a time from `src/`:

```
spark-submit --py-files common.py ingest_to_hdfs.py data_Q1_2026
spark-submit --py-files common.py clean_normalize.py
spark-submit --py-files common.py feature_engineering.py
spark-submit --py-files common.py train_model.py
spark-submit --py-files common.py --jars ~/jars/clickhouse-jdbc-0.6.3-all.jar serving_layer.py
spark-submit --py-files common.py --jars ~/jars/clickhouse-jdbc-0.6.3-all.jar load_model_tables.py
spark-submit --py-files common.py --jars ~/jars/clickhouse-jdbc-0.6.3-all.jar stream_anomaly_scores.py --once
```

Runtimes on the full Q1 2026 data: ingest 4 min, clean 6 min, features 16 min (84 models), training 8.5 min, serving 8 min, model tables 1 min, streaming one day (345,662 readings) 2.5 min.

## 9. Web UIs

Nothing is open to the internet. From a laptop:

```
gcloud compute ssh master -- -N -L 9870:localhost:9870 -L 8080:localhost:8080 -L 18080:localhost:18080 -L 4040:localhost:4040 -L 8123:localhost:8123
```

Then http://localhost:9870 (HDFS), :8080 (Spark master), :18080 (history), :4040 (running job), :8123/play (ClickHouse).

## 10. Stopping and starting

Stop when nobody is using it; stopped VMs only pay for disk.

```
gcloud compute instances stop master worker1 worker2
gcloud compute instances start master worker1 worker2
```

After a start, on the master as `hadoop`:

```
start-dfs.sh && start-master.sh && start-workers.sh && start-history-server.sh
sudo clickhouse start
```

## Problems we hit

| Problem | Cause and fix |
| --- | --- |
| Spark download took an hour | archive.apache.org throttles; use the dlcdn link for the current 3.5.x |
| `clickhouse-client` broken pipe on port 9000 | That port is the HDFS NameNode; ClickHouse native port moved to 9001 |
| `Provided Maven Coordinates must be in the form` | `spark-submit --packages` cannot fetch the shaded driver; use `--jars` with the downloaded jar |
| `Code: 57 ... already exists` from Spark's JDBC writer | `?compress=0` missing from the JDBC URL |
| Streaming scored 96% of the fleet as failing | Label leakage in training: the last 30 days kept only failing drives. Fixed by dropping the whole window |
