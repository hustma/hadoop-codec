#!/bin/bash

defaultLoop=2
inputPath=/path/to/log/%Y/%m/%d/

checkPathExists() {
  hadoop fs -ls $1 >/dev/null
  if [ $? -ne 0 ]; then
    return 1
  else
    return 0
  fi
}

checkPath() {
    checkPathExists $1
    if [ $? -ne 0 ]; then
        return 1
    else
       checkPathExists $1"/lzo"
       if [ $? -ne 0 ]; then
          return 0
       else
          return 1
       fi
    fi
}

submit() {
    checkPath $1
    if [ $? -gt 0 ]; then
        echo $1 >> empty_path.log
    else
        hadoop jar hadoop-codec-1.0.0-SNAPSHOT.jar com.fxiaoke.dataplatform.mapreduce.LzoCompressMain -Dfile.pattern=.*.log $1 $1"/lzo"
    fi
}

batchSubmit() {
    startDate=$1
    endDate=$2

    dateCursor=${startDate}
    loop=0
    until [[ "$dateCursor" > "$endDate" ]]
    do
        loop=$(expr ${loop} + 1)
        INPUT=$(date -d"$dateCursor" +"$inputPath")
        echo "submit $INPUT $loop"
        submit ${INPUT} &
            if [ ${loop} -ge ${defaultLoop} ]; then
            wait
            loop=0
        fi
        dateCursor=$(date -d"$dateCursor 1 day" +"%F %T")
    done
}

batchSubmit 20160801 20160901