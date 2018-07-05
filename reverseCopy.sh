#!/bin/bash
DBNAME=dl_prime2
curdate=$(date "+%Y%m%d-%H%M%S")
kinit sysafhdlafprm2 -kt /home/sysafhdlafprm2/sysafhdlafprm2.keytab
active=` curl  -u infa:infa -k -sS -G  "https://jhbpsr020000101.intranet.barcapint.com:8443/api/v1/clusters/Galactic/host_components?HostRoles/component_name=NAMENODE&metrics/dfs/FSNamesystem/HAState=active"| grep 'host_name'`
activehost=`echo $active | cut -d ":" -f 2  |  tr -d '"'`
activehost="$(echo -e "${activehost}" | tr -d '[:space:]')"
copycmd="hadoop distcp   -skipcrccheck   -update  webhdfs://${activehost}:50070/bigdatahdfs/datalake/publish/prm2 /bigdatahdfs/datalake/publish/prm2"
echo "Command : $copycmd "
$copycmd

if [ $? -eq 0 ] ; then
        echo "Copy command completed successfully"  
else
        echo "Copy command failed."  | mail -s "ReverseCopy status for Card-Interchange Landing Layer" BAGL_HADOOP_ADMIN@absa.co.za
fi


export DR_BEELINE_CONNECT_STRING="\"jdbc:hive2://jhbpsr000001017.intranet.barcapint.com:10000/$DBNAME;principal=hive/_HOST@INTRANET.BARCAPINT.COM\""
export PROD_BEELINE_CONNECT_STRING="\"jdbc:hive2://jhbpsr020000142.intranet.barcapint.com:10000/$DBNAME;principal=hive/_HOST@INTRANET.BARCAPINT.COM\""
#hive --database  $DBNAME  -S  -e "show tables " > /tmp/$DBNAME_$curdate.txt
tablenames="/home/sysafhdlafprm2/${DBNAME}_${curdate}.txt"
countfile="/home/sysafhdlafprm2/count_${DBNAME}_${curdate}.log"
beeline -u "${PROD_BEELINE_CONNECT_STRING}" -n hive -p hive  --silent=true  --showHeader=false --outputformat=csv2 -e "show tables" >>  ${tablenames}
IFS=$'\n'       # make newtables the only separator
set -f
#filetables=`cat /tmp/$DBNAME_$curdate.txt`
for table in $(cat < "$tablenames");do
  echo " table : $table"
  table=`echo $table | xargs`

        if [ $table != "log4j" ] ; then
        echo "Repair table"
        beeline -u "${DR_BEELINE_CONNECT_STRING}" -e "msck repair table ${table}"

        drcount=`beeline -u "${DR_BEELINE_CONNECT_STRING}" -e 'select count(*) from '${table}''`

        drcount=`echo $drcount | sed -e 's/\<_c0\>//g'| grep -om1 '[0-9]\+'`
        echo " drcount : $drcount "

        prodcount=`beeline -u "${PROD_BEELINE_CONNECT_STRING}" -e 'select count(*) from '${table}''`
        prodcount=`echo $prodcount | sed -e 's/\<_c0\>//g'| grep -om1 '[0-9]\+'`
        echo "prodcount : $prodcount"

        if [ "$drcount" -eq  "$prodcount" ] ; then
        echo "[ $table  $drcount        $prodcount      MATCH]" >> ${countfile}
        else
        echo "[ $table  $drcount        $prodcount      DOESNOT MATCH]" >> ${countfile}
        fi
        fi

done
content=`cat ${countfile}` ; echo "$content"
echo "$content}"  | mail -s "ReverseCopy status for Prime2 Source" BAGL_HADOOP_ADMIN@absa.co.za
