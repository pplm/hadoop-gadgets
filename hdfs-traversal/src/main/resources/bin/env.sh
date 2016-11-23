
hadoopHome=${HADOOP_HOME}
hadoopLib=${hadoopHome}/share/hadoop
hadoopCommon=${hadoopLib}/common
hadoopHdfs=${hadoopLib}/hdfs
hadoopMapreduce=${hadoopLib}/mapreduce
hadoopYarn=${hadoopLib}/yarn

export hadoopCmd=hadoop
export javaCmd=java
export configPath=./conf
export logPath=./logs
export outputPath=./output

export hdfsClasspath=.:${hadoopCommon}/*:${hadoopCommon}/lib/*:${hadoopHdfs}/*:${hadoopHdfs}/lib/*:./lib/*:${configPath}/.
export mrClasspath=.:${hadoopCommon}/*:${hadoopCommon}/lib/*:${hadoopHdfs}/*:${hadoopHdfs}/lib/*:${hadoopMapreduce}/*:${hadoopYarn}/*:./lib/*:./lib/.:${configPath}/.
export classpath=${mrClasspath}

mkdir -p ${logPath}

