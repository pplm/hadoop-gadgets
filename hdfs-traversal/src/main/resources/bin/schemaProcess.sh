#!/bin/bash

cd ${0%/*}

if [ $# -lt 2 ]; then
    echo "Usage $0 <hdfs src> <local dst>"
    exit 1
fi

hdfsInputPath=$1
localOutputPath=$2

source env.sh

hdfsEccFile="/tmp/ecc.csv"
eccFile="${configPath}/ecc.csv"

logFile=${logPath}/schemaProcess.log

echo "==============="$(date "+%Y-%m-%d %H:%M:%S")" schema process job begin ==============" >> ${logFile}

${hadoopCmd} fs -rm ${hdfsEccFile}
${hadoopCmd} fs -put ${eccFile} ${hdfsEccFile}

${javaCmd} -classpath ${mrClasspath} com.k2data.bdpe.hdfs.schema.SchemaProcess ${hdfsInputPath} ${localOutputPath} >> ${logFile} 2>&1 &

echo $(date "+%Y-%m-%d %H:%M:%S")" schema process job begin"

