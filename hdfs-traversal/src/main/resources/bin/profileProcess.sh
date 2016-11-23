#!/bin/bash
cd ${0%/*}

if [ $# -lt 1 ]; then
    echo "Usage $0 <hdfs path>"
    exit 1
fi

hdfsInputPath=$1
name=profileProcess
class=com.k2data.bdpe.hdfs.profile.ProfileProcess

source env.sh
localOutputPath=${outputPath}/${name}/.
mkdir -p ${localOutputPath}
logFile=${logPath}/${name}.log

echo "==============="$(date "+%Y-%m-%d %H:%M:%S")" ${name} job begin ==============" >> ${logFile}

${javaCmd} -classpath ${mrClasspath} ${class} ${hdfsInputPath} ${localOutputPath} >> ${logFile} 2>&1 &
echo $(date "+%Y-%m-%d %H:%M:%S"): ${name} job begin




