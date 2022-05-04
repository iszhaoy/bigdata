#!/bin/bash
java -cp ../target/java-demo-1.0-SNAPSHOT-jar-with-dependencies.jar com.iszhaoy.CliTest $* http > ../target/result

if [ $? -eq 0 ] 
 then 
  while read line 
  do 
    echo $line 

  done < ../target/result 
fi
