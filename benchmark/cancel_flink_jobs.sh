#/bin/bash

echo "Cancelling all flink jobs"

cd flink-1.19.0/bin

jobs=`./flink list | awk {'print $4'} | egrep  '^\w+$'`

for i in $jobs
    do
        echo -e Cancelling $i
        ./flink cancel $i
    done

echo "All jobs cancelled successfully"