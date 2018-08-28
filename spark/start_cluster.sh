#!/bin/bash
aws s3 cp emr_bootstrap.sh s3://emrdata.cancerdatasci.org/omicidx/emr_bootstrap.sh
aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole --applications Name=Hadoop Name=Zeppelin Name=Ganglia Name=Spark Name=MXNet Name=Livy --ebs-root-volume-size 10 --ec2-attributes '{"KeyName":"EveryDay","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-0ba0c452","EmrManagedSlaveSecurityGroup":"sg-5ee5d72b","EmrManagedMasterSecurityGroup":"sg-fde5d788"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.16.0 --log-uri 's3n://aws-logs-377200973048-us-east-1/elasticmapreduce/' --name 'My cluster' --instance-groups '[{"InstanceCount":4,"BidPrice":"0.035","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"m4.large","Name":"Core - 2"},{"InstanceCount":1,"BidPrice":"0.070","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":150,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"MASTER","InstanceType":"m4.xlarge","Name":"Master - 1"}]' --configurations '[{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"},"Configurations":[]}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1 --configurations file://emr-config.json --bootstrap-action Path=s3://emrdata.cancerdatasci.org/omicidx/emr_bootstrap.sh \
    --bootstrap-action Path=s3://aws-bigdata-blog/artifacts/aws-blog-emr-rstudio-sparklyr/rstudio_sparklyr_emr5.sh,Args=["--rstudio","--shiny","--sparkr","--rexamples","--plyrmr","--rhdfs","--sparklyr"],Name="Install RStudio"
