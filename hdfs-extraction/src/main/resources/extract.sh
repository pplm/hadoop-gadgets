#!/bin/bash
cd ${0%/*}

if [ $# -lt 2 ]; then
    echo "Usage $0 <hdfs src> <local path dest>"
    exit 1
fi

hadoopHome=/home/fit/hadoop
hadoopCommon=${hadoopHome}/share/hadoop/common
hadoopHdfs=${hadoopHome}/share/hadoop/hdfs
configPath=./conf
hdfsSrc=$1
localDst=$2

java -DconfigFile=./conf/config.properties -classpath .:${hadoopCommon}/*:${hadoopCommon}/lib/*:${hadoopHdfs}/*:${hadoopHdfs}/lib/*:./lib/*:${configPath}/. com.k2data.bdpe.hdfs.extraction.Main ${hdfsSrc} ${localDst} &
echo $(date "+%Y-%m-%d %H:%M:%S")" extraction begin"
