.PHONY: \
	build \
	clean \
	hadoop_base \
	hadoop_namenode \
	hadoop_datanode \
	hadoop_yarn_resourcemanager \
	hadoop_yarn_nodemanager \
	spark \
	zookeeper \
	hbase_base \
	hbase_master \
	hbase_regionserver \
	postgres \
	livy \
	bda_devel

build: \
	hadoop_base \
	hadoop_namenode \
	hadoop_datanode \
	hadoop_yarn_resourcemanager \
	hadoop_yarn_nodemanager \
	spark \
	zookeeper \
	hbase_base \
	hbase_master \
	hbase_regionserver \
	postgres \
	livy \
	bda_devel


include .env
export

hadoop_base:
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--tag datasource/hadoop-base-$(HADOOP_VERSION) \
		./hadoop/base

hadoop_namenode: hadoop_base
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--tag datasource/hadoop-namenode-$(HADOOP_VERSION) \
		./hadoop/namenode

hadoop_datanode: hadoop_base
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--tag datasource/hadoop-datanode-$(HADOOP_VERSION) \
		./hadoop/datanode

hadoop_yarn_resourcemanager: hadoop_base 
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--tag datasource/hadoop-yarn-resourcemanager-$(HADOOP_VERSION) \
		./hadoop/resourcemanager

hadoop_yarn_nodemanager: hadoop_base
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--tag datasource/hadoop-yarn-nodemanager-$(HADOOP_VERSION) \
		./hadoop/nodemanager

spark: hadoop_yarn_nodemanager
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--build-arg SPARK_VERSION=$(SPARK_VERSION) \
                --build-arg HBASE_VERSION=$(HBASE_VERSION) \
		--tag datasource/spark-$(SPARK_VERSION) \
		./spark

zookeeper: hadoop_base
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--build-arg ZOOKEEPER_VERSION=$(ZOOKEEPER_VERSION) \
		--tag datasource/zookeeper-$(ZOOKEEPER_VERSION) \
		./zookeeper

hbase_base: hadoop_base
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--build-arg HBASE_VERSION=$(HBASE_VERSION) \
		--tag datasource/hbase-base-$(HBASE_VERSION) \
		./hbase/base

hbase_master: hbase_base
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--build-arg HBASE_VERSION=$(HBASE_VERSION) \
		--tag datasource/hbase-master-$(HBASE_VERSION) \
		./hbase/master

hbase_regionserver: hbase_base
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--build-arg HBASE_VERSION=$(HBASE_VERSION) \
		--tag datasource/hbase-regionserver-$(HBASE_VERSION) \
		./hbase/regionserver

postgres:
	docker build \
		--tag datasource/postgres-11 \
		./postgres

livy: spark
	docker build \
		--build-arg SPARK_VERSION=$(SPARK_VERSION) \
		--build-arg LIVY_VERSION=$(LIVY_VERSION) \
		--tag datasource/livy-$(LIVY_VERSION) \
		./livy

bda: spark
	docker build \
		--build-arg SPARK_VERSION=$(SPARK_VERSION) \
		--build-arg BDA_VERSION=$(BDA_VERSION) \
		--build-arg BDA_PREFIX=$(BDA_PREFIX) \
		--file ./bda/Dockerfile \
		--tag datasource/bda-$(BDA_VERSION) \
		./bda

bda_devel: spark
	docker build \
		--build-arg SPARK_VERSION=$(SPARK_VERSION) \
		--file ./bda/Dockerfile.devel \
		--tag datasource/bda-devel \
		./bda

clean:
	docker rmi \
		datasource/hadoop-base-$(HADOOP_VERSION) \
		datasource/hadoop-namenode-$(HADOOP_VERSION) \
		datasource/hadoop-datanode-$(HADOOP_VERSION) \
		datasource/hadoop-yarn-resourcemanager-$(HADOOP_VERSION) \
		datasource/hadoop-yarn-nodemanager-$(HADOOP_VERSION) \
		datasource/spark-$(SPARK_VERSION) \
		datasource/zookeeper-$(ZOOKEEPER_VERSION) \
		datasource/hbase-base-$(HBASE_VERSION) \
		datasource/hbase-master-$(HBASE_VERSION) \
		datasource/hbase-regionserver-$(HBASE_VERSION) \
		datasource/postgres-11 \
		datasource/livy-$(LIVY_VERSION) \
		datasource/bda-devel
