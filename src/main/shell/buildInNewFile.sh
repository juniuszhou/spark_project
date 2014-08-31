#!/bin/sh
spath="/home/junius/git_hub/spark_project/src/main/scala/"
dpath="/home/junius/develop/spark-1.0.0/examples/target/scala-2.10/classes/org/apache/spark/examples/"

postfix=".scala"
wildPost="*"

sfile=${spath}${1}${postfix}
dfile=${dpath}${1}${postfix}
allOldFiles=${dpath}${1}${wildPost}
echo $dfile

rm $allOldFiles
cp $sfile $dfile

# exec sbt to get example package
sbtpath="/home/junius/develop/spark-1.0.0"
cd $sbtpath
sbt/sbt -mem 1024m assembly

