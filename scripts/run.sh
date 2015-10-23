fecha=$(date +"%d_%m_%Y")
inicio=`date +%s`

hdfs dfs -rm -r /reconstruccion/rvasexac_id/
hdfs dfs -rm -r /reconstruccion/base_r1_spark/
hdfs dfs -rm -r /reconstruccion/base_r1_casos_spark/

spark-submit \
        --class mx.com.gnp.App \
        --properties-file spark.conf \
        --deploy-mode client \
        --num-executors 3 \
        --driver-memory 14g \
        --executor-memory 14g \
        --executor-cores 8 \
        AsignacionR1-0.0.1-SNAPSHOT.jar


hdfs dfs -chmod 777 /reconstruccion/rvasexac_id/
hdfs dfs -chmod 777 /reconstruccion/base_r1_spark/
hdfs dfs -chmod 777 /reconstruccion/base_r1_casos_spark/

impala-shell -i dn1.pilot.gnp -f createTables.sql

hdfs dfs -rm -r /reconstruccion/rvasexac_id/
hdfs dfs -rm -r /reconstruccion/base_r1_spark/
hdfs dfs -rm -r /reconstruccion/base_r1_casos_spark/

fin=`date +%s`
let total=$fin-$inicio
echo "Proceso completado en ::: $total segundos"