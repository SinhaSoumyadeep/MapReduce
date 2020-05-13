# Makefile for Hadoop MapReduce WordCount demo project.

# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/private/etc/hadoop-2.9.0
jar.name=mr-demo-1.0.jar
jar.path=target/${jar.name}
job.name=wc.ReduceSideJoin
job2.name = wc.ReplicatedJoin
job3.name = wc.MaxFilter
job4.name = wc.Cardinality
main.input = mainInput
local.input=input
local.output=output
local.log = log
max.filter = 12000000
# Pseudo-Cluster Execution
hdfs.user.name=soumyadeep
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-5.29.0
aws.region=us-east-1
aws.bucket.name=soumyadeep-assignment2
aws.subnet.id=subnet-6356553a
aws.maininput=maininput
aws.input=input
aws.repoutput=rep-output
aws.redoutput=red-output
aws.log.dir=log
aws.num.nodes=7
aws.instance.type=m5.xlarge
aws.local.dir = awslocal
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
redjoin: jar clean-local-output
	rm -rf temp_file
	rm -rf temp_intermediate
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${local.input} ${local.output}

repjoin: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job2.name} ${main.input} ${local.output} ${max.filter}

maxfilter: jar clean-local-output
	rm -rf ${local.input}*
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job3.name} ${main.input} ${local.input} ${max.filter}

cardinality: jar clean-local-output
	rm -rf temp
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job4.name} ${main.input} ${local.output} ${max.filter}


# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs: 
	${hadoop.root}/sbin/stop-dfs.sh
	
# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.	
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /Users/soumyadeepsinha/Desktop/user
	${hadoop.root}/bin/hdfs dfs -mkdir /Users/soumyadeepsinha/Desktop/user
	${hadoop.root}/bin/hdfs dfs -mkdir /Users/soumyadeepsinha/Desktop/user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /Users/soumyadeepsinha/Desktop/user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /Users/soumyadeepsinha/Desktop/user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output-hdfs: clean-local-output
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get /Users/soumyadeepsinha/Desktop/user/${hdfs.user.name}/${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} /Users/soumyadeepsinha/Desktop/user/${hdfs.user.name}/${hdfs.input} /Users/soumyadeepsinha/Desktop/user/${hdfs.user.name}/${hdfs.output}
	make download-output-hdfs

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output-hdfs

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${main.input} s3://${aws.bucket.name}/${aws.maininput}

	
# Delete S3 output dir.
delete-tempinput-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.input}*"

delete-redoutput-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.redoutput}*"

delete-repoutput-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.repoutput}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

# Main EMR launch.
awsfilter: jar upload-app-aws delete-tempinput-aws
	aws emr create-cluster \
		--name "Max Filter New" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job3.name}","s3://${aws.bucket.name}/${aws.maininput}","s3://${aws.bucket.name}/${aws.input}", "${max.filter}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate

awsrep: jar upload-app-aws delete-repoutput-aws
	aws emr create-cluster \
		--name "Replicated Join 10000-7" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job2.name}","s3://${aws.bucket.name}/${aws.maininput}","s3://${aws.bucket.name}/${aws.repoutput}", "${max.filter}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate

awsred: jar upload-app-aws delete-redoutput-aws
	aws emr create-cluster \
		--name "Reduce Side Join 20000-7" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.name}","s3://${aws.bucket.name}/${aws.maininput}","s3://${aws.bucket.name}/${aws.redoutput}", "${max.filter}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate


# Download output from S3.
download-output-aws:
	rm -rf ${aws.local.dir}/${local.output}*
	mkdir ${aws.local.dir}/${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.input} ${aws.local.dir}/${local.output}

# Download log from S3.
download-bucket-aws:
	rm -rf ${aws.local.dir}*
	mkdir ${aws.local.dir}
	aws s3 sync s3://${aws.bucket.name} ${aws.local.dir}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -f MR-Demo.tar.gz
	rm -f MR-Demo.zip
	rm -rf build
	mkdir -p build/deliv/MR-Demo
	cp -r src build/deliv/MR-Demo
	cp -r config build/deliv/MR-Demo
	cp -r input build/deliv/MR-Demo
	cp pom.xml build/deliv/MR-Demo
	cp Makefile build/deliv/MR-Demo
	cp README.txt build/deliv/MR-Demo
	tar -czf MR-Demo.tar.gz -C build/deliv MR-Demo
	cd build/deliv && zip -rq ../../MR-Demo.zip MR-Demo
