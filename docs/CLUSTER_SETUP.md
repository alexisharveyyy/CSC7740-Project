# Cluster Setup: HDFS + Spark Standalone + ClickHouse on Google Cloud

Three Ubuntu 22.04 VMs on the GCP free trial ($300, 90 days). One master, two workers.

| Node | Roles | Shape |
| --- | --- | --- |
| `master` | HDFS NameNode, Spark master, ClickHouse, spark-submit driver | e2-standard-2 (2 vCPU, 8 GB), 100 GB disk |
| `worker1` | HDFS DataNode, Spark worker | e2-standard-2 (2 vCPU, 8 GB), 100 GB disk |
| `worker2` | HDFS DataNode, Spark worker | e2-standard-2 (2 vCPU, 8 GB), 100 GB disk |

Cost while running: roughly $0.25 per hour for all three, about $180 per month if never stopped.
Stop the VMs whenever nobody is using them (Part 9). Stopped VMs only pay for disk, about $30 per month total.

Versions: OpenJDK 17, Hadoop 3.3.6, Spark 3.5.1 (hadoop3 build), ClickHouse latest stable, Python 3.10 (Ubuntu 22.04 default; compatible with pyspark 3.5.1 and the repo).

Conventions: `$` means run on your laptop, `master$` means run on the master VM, `all$` means run on every VM.

---

## Part 1: Google Cloud account and project

1. Go to https://cloud.google.com/free and start the free trial with your LSU Google account or a personal Gmail. Enter the card; it is for verification only.
2. In the console, create a project named `csc7740-hdd` and select it.
3. Enable the Compute Engine API (Navigation menu > Compute Engine > VM instances; it prompts you to enable it).
4. Install the gcloud CLI on your laptop: https://cloud.google.com/sdk/docs/install. Then:

```
$ gcloud auth login
$ gcloud config set project csc7740-hdd
$ gcloud config set compute/zone us-central1-a
```

## Part 2: Create the VMs

Create all three with one command each. Internal DNS on GCP resolves instance names automatically, so `master`, `worker1`, and `worker2` work as hostnames inside the VPC with no `/etc/hosts` editing.

```
$ for name in master worker1 worker2; do
    gcloud compute instances create $name \
      --machine-type=e2-standard-2 \
      --image-family=ubuntu-2204-lts \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size=100GB \
      --boot-disk-type=pd-balanced \
      --tags=hadoop
  done
```

Firewall: the default VPC already allows all traffic between VMs and SSH from the internet. Do not open the web UIs to the internet; use SSH tunnels (Part 8).

Confirm SSH works and note that it also sets up your SSH key the first time:

```
$ gcloud compute ssh master
```

## Part 3: Base packages on every VM

Run this on all three. Open three terminals, or run each in turn with `gcloud compute ssh <name>`.

```
all$ sudo apt-get update
all$ sudo apt-get install -y openjdk-17-jdk-headless python3-pip python3-venv python3-numpy python3-pandas unzip curl
all$ java -version
```

Workers run PySpark tasks with the system `python3`, which is why numpy and pandas are installed system-wide. The repo's venv lives only on the master for the driver.

Create a `hadoop` user on all three so paths and permissions match everywhere:

```
all$ sudo adduser --disabled-password --gecos "" hadoop
all$ sudo usermod -aG sudo hadoop
all$ echo "hadoop ALL=(ALL) NOPASSWD:ALL" | sudo tee /etc/sudoers.d/hadoop
all$ sudo su - hadoop
```

Everything from here on runs as the `hadoop` user.

## Part 4: Passwordless SSH from master to workers

Hadoop and Spark start remote daemons over SSH from the master.

```
master$ ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
master$ cat ~/.ssh/id_ed25519.pub
```

Copy that public key line, then on each worker (and the master itself):

```
all$ mkdir -p ~/.ssh && chmod 700 ~/.ssh
all$ echo "<paste the public key line>" >> ~/.ssh/authorized_keys
all$ chmod 600 ~/.ssh/authorized_keys
```

Verify from the master; each must log in without a prompt:

```
master$ ssh -o StrictHostKeyChecking=accept-new master hostname
master$ ssh -o StrictHostKeyChecking=accept-new worker1 hostname
master$ ssh -o StrictHostKeyChecking=accept-new worker2 hostname
```

## Part 5: Hadoop HDFS

### 5.1 Install on every VM

```
all$ cd ~
all$ curl -LO https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
all$ tar xzf hadoop-3.3.6.tar.gz && rm hadoop-3.3.6.tar.gz
all$ mv hadoop-3.3.6 hadoop
all$ sudo mkdir -p /data/hdfs/namenode /data/hdfs/datanode && sudo chown -R hadoop:hadoop /data
```

Append to `~/.bashrc` on every VM, then `source ~/.bashrc`:

```
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=$HOME/spark
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3
```

### 5.2 Configuration (identical on every VM)

`~/hadoop/etc/hadoop/hadoop-env.sh`: add this line near the top.

```
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

`~/hadoop/etc/hadoop/core-site.xml`:

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://master:9000</value>
  </property>
</configuration>
```

`~/hadoop/etc/hadoop/hdfs-site.xml`:

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///data/hdfs/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///data/hdfs/datanode</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-bind-host</name>
    <value>0.0.0.0</value>
  </property>
</configuration>
```

`~/hadoop/etc/hadoop/workers` (replace the file's contents):

```
worker1
worker2
```

Fast way to copy config to the workers once the master's is right:

```
master$ for w in worker1 worker2; do scp ~/hadoop/etc/hadoop/{hadoop-env.sh,core-site.xml,hdfs-site.xml,workers} $w:~/hadoop/etc/hadoop/; done
```

### 5.3 Format and start

```
master$ hdfs namenode -format -force
master$ start-dfs.sh
master$ jps
```

`jps` on the master should show `NameNode` and `SecondaryNameNode`; on each worker, `DataNode`. Confirm both DataNodes registered:

```
master$ hdfs dfsadmin -report | grep -E "Live datanodes|Name:"
master$ hdfs dfs -mkdir -p /backblaze/raw
```

If a DataNode is missing, check `~/hadoop/logs/hadoop-hadoop-datanode-worker1.log` on that worker.

## Part 6: Spark standalone

### 6.1 Install on every VM

```
all$ cd ~
all$ curl -LO https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
all$ tar xzf spark-3.5.1-bin-hadoop3.tgz && rm spark-3.5.1-bin-hadoop3.tgz
all$ mv spark-3.5.1-bin-hadoop3 spark
```

### 6.2 Configuration (identical on every VM)

`~/spark/conf/spark-env.sh`:

```
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_CONF_DIR=$HOME/hadoop/etc/hadoop
export SPARK_MASTER_HOST=master
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=6g
export PYSPARK_PYTHON=python3
```

`~/spark/conf/spark-defaults.conf`:

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

`~/spark/conf/workers`:

```
worker1
worker2
```

Copy to workers and create the event log directory:

```
master$ for w in worker1 worker2; do scp ~/spark/conf/{spark-env.sh,spark-defaults.conf,workers} $w:~/spark/conf/; done
master$ hdfs dfs -mkdir -p /spark-logs
```

### 6.3 Start and smoke test

```
master$ start-master.sh
master$ start-workers.sh
master$ start-history-server.sh
master$ jps
```

Master shows `Master` and `HistoryServer`; workers show `Worker`. Run a job that touches both workers and HDFS:

```
master$ spark-submit --master spark://master:7077 $SPARK_HOME/examples/src/main/python/pi.py 100 2>&1 | grep "Pi is"
```

Open the Spark master UI through a tunnel (Part 8) and confirm 2 workers with 4 cores total.

## Part 7: ClickHouse on the master

```
master$ curl https://clickhouse.com/ | sh
master$ sudo ./clickhouse install
```

The installer asks for a default-user password; press Enter to leave it empty (the cluster is not internet-facing). Make it listen on the internal interface so Spark executors on the workers can write to it:

```
master$ sudo tee /etc/clickhouse-server/config.d/listen.xml > /dev/null <<'EOF'
<clickhouse>
  <listen_host>0.0.0.0</listen_host>
</clickhouse>
EOF
master$ sudo clickhouse start
master$ clickhouse-client --query "CREATE DATABASE IF NOT EXISTS backblaze"
master$ clickhouse-client --query "SELECT version()"
```

Verify a worker can reach it:

```
worker1$ curl -s http://master:8123/ping
```

Expected output: `Ok.`

## Part 8: SSH tunnels for the web UIs

Run on your laptop; leave the terminal open while you use the UIs.

```
$ gcloud compute ssh master -- -N -L 9870:localhost:9870 -L 8080:localhost:8080 -L 18080:localhost:18080 -L 4040:localhost:4040 -L 8123:localhost:8123
```

| URL on your laptop | What it is |
| --- | --- |
| http://localhost:9870 | HDFS NameNode (Datanodes tab shows the 2 workers; use this in the video) |
| http://localhost:8080 | Spark master (workers, running and completed apps) |
| http://localhost:18080 | Spark history server (past jobs, stages, data read per task) |
| http://localhost:4040 | Spark UI of the job currently running |
| http://localhost:8123/play | ClickHouse query UI |

## Part 9: Stopping and starting (do this every session)

Stop when done (keeps disks, stops compute billing):

```
$ gcloud compute instances stop master worker1 worker2
```

Start again and bring the services up:

```
$ gcloud compute instances start master worker1 worker2
$ gcloud compute ssh master
master$ sudo su - hadoop
master$ start-dfs.sh && start-master.sh && start-workers.sh && start-history-server.sh
master$ sudo clickhouse start
master$ hdfs dfsadmin -report | grep "Live datanodes"
```

Internal names stay the same after a restart. External IPs change unless you reserve them, which matters only for the GitHub deploy workflow (Part 11).

## Part 10: Repo and data on the master

### 10.1 Clone and set up the environment

```
master$ git clone https://github.com/alexisharveyyy/CSC7740-Project.git ~/CSC7740-Project
master$ cd ~/CSC7740-Project
master$ python3 -m venv .venv && source .venv/bin/activate
master$ pip install -r requirements.txt
```

Add to the `hadoop` user's `~/.bashrc` on the master:

```
export BACKBLAZE_HDFS_ROOT=hdfs://master:9000/backblaze
export CLICKHOUSE_JDBC_URL=jdbc:clickhouse://master:8123/backblaze
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=
```

### 10.2 Load the Backblaze quarter into HDFS

Get the Q1 2026 archive link from https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data (right-click the download button, copy link). A quarter is about 2 GB zipped and 10 to 15 GB of CSV, so it fits on the master's disk and in HDFS with replication 2.

```
master$ cd ~/CSC7740-Project
master$ scripts/download_backblaze.sh "<archive url>" data_Q1_2026
master$ scripts/load_raw_to_hdfs.sh data_Q1_2026
master$ rm -rf data_Q1_2026
```

The download script flattens nested folders, and `data_*/` is gitignored so the CSVs can sit in the repo directory without being committed.

### 10.3 Run the pipeline

The short way, once `.env` is in place (copy `.env.example` to `.env` and `source` it):

```
master$ cd ~/CSC7740-Project && source .venv/bin/activate && source .env
master$ scripts/run_pipeline.sh
```

Stage by stage, which is what `run_pipeline.sh` does internally:

```
master$ cd ~/CSC7740-Project/src && source ../.venv/bin/activate
master$ spark-submit --py-files common.py ingest_to_hdfs.py data_Q1_2026
master$ spark-submit --py-files common.py clean_normalize.py
master$ spark-submit --py-files common.py feature_engineering.py
master$ spark-submit --py-files common.py train_model.py
master$ spark-submit --py-files common.py --packages com.clickhouse:clickhouse-jdbc:0.6.3:all serving_layer.py
master$ spark-submit --py-files common.py --packages com.clickhouse:clickhouse-jdbc:0.6.3:all load_model_tables.py
master$ clickhouse-client --query "SELECT table, total_rows FROM system.tables WHERE database = 'backblaze'"
```

`spark.master` comes from `spark-defaults.conf`, so no `--master` flag is needed. Watch http://localhost:4040 during a run: the Executors tab should list both workers, which is the evidence for the "not a single node" requirement.

Streaming demo:

```
master$ hdfs dfs -mkdir -p /backblaze/stream/incoming
master$ spark-submit --py-files common.py --packages com.clickhouse:clickhouse-jdbc:0.6.3:all stream_anomaly_scores.py &
master$ hdfs dfs -put <some daily csv> /backblaze/stream/incoming/
master$ clickhouse-client --query "SELECT count() FROM backblaze.streaming_anomaly_scores"
```

## Part 11: GitHub Actions deploy (optional)

The deploy workflow SSHes into the master. It needs a stable address, so reserve the master's external IP:

```
$ gcloud compute addresses create master-ip --region=us-central1
$ gcloud compute instances delete-access-config master --access-config-name="external-nat"
$ gcloud compute instances add-access-config master --address=$(gcloud compute addresses describe master-ip --region=us-central1 --format="value(address)")
```

Then create a deploy key on your laptop, add the public half to `~hadoop/.ssh/authorized_keys` on the master, and set the repo secrets the workflow expects (host = the reserved IP, user = `hadoop`, private key, plus the `CLICKHOUSE_*` values from Part 10.1). A reserved IP costs about $7 per month while the VM is stopped, so release it at the end of the semester.

## Part 12: What to document in the report

- Topology table from the top of this file, plus a screenshot of the HDFS Datanodes page and the Spark master page.
- `hdfs dfsadmin -report` output showing 2 live DataNodes and replication 2.
- `hdfs fsck /backblaze/conformed -files -blocks -locations | head` showing blocks spread across both workers.
- Spark history server page for one pipeline job showing tasks on both executors.
- Total cost from the GCP billing page at the end, which will be well under the $300 credit.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `start-dfs.sh` asks for a password | Part 4 key not in that node's `authorized_keys`, or wrong permissions (700 on `.ssh`, 600 on the file) |
| 0 live DataNodes after `-format` | Cluster ID mismatch. On each worker: `rm -rf /data/hdfs/datanode/*`, then restart |
| Worker not in Spark master UI | `SPARK_MASTER_HOST` wrong or `spark-env.sh` not copied; check `~/spark/logs` on the worker |
| Executors die with `OutOfMemoryError` | Lower `spark.executor.memory` to 4g, or raise `spark.sql.shuffle.partitions` to 32 |
| `No suitable driver` or JDBC timeout from executors | ClickHouse `listen_host` not set (Part 7), or `CLICKHOUSE_JDBC_URL` uses `localhost` instead of `master` |
| `--packages` hangs | Master has no internet route (unlikely on GCP defaults). Download `clickhouse-jdbc-0.6.3-all.jar` once and use `--jars` instead |
| Python version mismatch error | `PYSPARK_PYTHON=python3` missing from `spark-env.sh` on a worker |
