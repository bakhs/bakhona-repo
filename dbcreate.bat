ug_prefix=${USER_GROUP:0:4}
user_g_bamboo=${bamboo.usergroup}

function user_group_and_prefix_func(){
        read USER_GROUP
        ug_prefix=${USER_GROUP:0:4}
}


HOST_NAME=$( echo "${bamboo.edge_server}" | tr '[:upper:]' '[:lower:]' )
DB_CREATE=$( echo "${bamboo.dbcreate}")
USER_GROUP=$( echo "${bamboo.usergroup}" )
LAYER=$( echo "${bamboo.layer}" )
COUNTRY_CODE=$( echo "${bamboo.country_code}" )
SOURCE=$( echo "${bamboo.source}" )
Business_ID=$( echo "${bamboo.Business_ID}" )

if [ $HOST_NAME == "jhbdsr000000393" ]
       then
          NAMESERVICE="hdfs://jhbdsr000000393:8020"
		  HIVE_SERVER="jhbdsr000000393"
fi
if [ $HOST_NAME == "jhbdsr020000014" ]
       then
          NAMESERVICE="hdfs://HdpAfrEnt"
		  HIVE_SERVER="jhbdsr020000017"
fi
if [ $HOST_NAME == "jhbdsr020000015" ]
       then
         NAMESERVICE="hdfs://HdpAfrEnt"
		 HIVE_SERVER="jhbdsr020000017"
fi
if [ $HOST_NAME == "jhbpsr020000061" ]
       then
          NAMESERVICE="hdfs://hdpafrpilot"
		  HIVE_SERVER="jhbpsr020000063"
fi
if [ $HOST_NAME == "jhbpsr020000062" ]
       then
          NAMESERVICE="hdfs://hdpafrpilot"
		  HIVE_SERVER="jhbpsr020000063"
fi
if [ $HOST_NAME == "jhbpsr020000101" ]
       then
          NAMESERVICE="hdfs://hdpafrprd"
		  HIVE_SERVER="jhbpsr020000142"
fi
if [ $HOST_NAME == "jhbpsr020000102" ]
       then
          NAMESERVICE="hdfs://hdpafrprd"
		  HIVE_SERVER="jhbpsr020000142"
fi
if [ $HOST_NAME == "jhbpsr000001015" ]
       then
          NAMESERVICE="hdfs://hdpafrdr"
		  HIVE_SERVER="jhbpsr000001017"
fi



export BEELINE_CONNECT_STRING="beeline -u 'jdbc:hive2://${HIVE_SERVER}.intranet.barcapint.com:10000/default;principal=hive/_HOST@INTRANET.BARCAPINT.COM' -n hive -p hive"
export ROLE=${DB_CREATE}_ROLE
export LOCATION=/bigdatahdfs/${LAYER}/${COUNTRY_CODE}/${Business_ID}/${SOURCE}/data
export LOCATION_ROLE=/bigdatahdfs/${LAYER}/${COUNTRY_CODE}/${Business_ID}/${SOURCE}


function pbrun_command_func(){
    arg="-h $HOST_NAME -u hdfs bash"
    echo " Executing PBRUN command with $arg"


expect <<END_EXPECT

    set RET_VAL 1
    set timeout 86400
     if {[regexp -nocase "jhbpsr020000061" $HOST_NAME ]} {
        set expectStr "hdfs@jhbpsr020000061*?$*"
    } elseif {[regexp -nocase  "jhbpsr020000062" $HOST_NAME ]} {
         set expectStr "hdfs@jhbpsr020000062*?$*"
    } elseif {[regexp -nocase "jhbpsr020000101" $HOST_NAME ]}  {
        set expectStr "jhbpsr020000101*?$*"
    } elseif {[regexp -nocase "jhbpsr000001015" $HOST_NAME ]}  {
        set expectStr "hdfs@jhbpsr000001015*DR*"
    } else  {
         set expectStr "bash-4.1$ "
    }

    set beelineStr "0: jdbc:hive2:*?>*"

    spawn pbrun  -h $HOST_NAME -u hdfs bash
    expect  "Active Directory Password:"
        send "${bamboo.password}\n"

    expect "\$expectStr"
        send "kinit hdfs/$HOST_NAME.intranet.barcapint.com -kt /etc/cdh-keytabs/ad_keytabs/hdfs-$HOST_NAME.keytab \n"

    expect "\$expectStr"
        send "hadoop fs -mkdir -p ${LOCATION_ROLE}\n" 
     
    expect "\$expectStr"
        send "hadoop fs -setfacl -R -m user:hive:rwx ${LOCATION_ROLE}\n"

     expect "\$expectStr"
        send "hadoop fs -setfacl -R -m user:impala:rwx ${LOCATION_ROLE}\n"

     expect "\$expectStr"
        send "kinit hive/${HOST_NAME}.intranet.barcapint.com -kt /etc/cdh-keytabs/ad_keytabs/hive-${HOST_NAME}.keytab\n"

     expect "\$expectStr"
        send "${BEELINE_CONNECT_STRING}\n"

     expect "\$beelineStr"
        send "create database ${DB_CREATE} location '${LOCATION}';\n"

     expect "\$beelineStr"
        send "create role ${ROLE};\n"

     expect "\$beelineStr"
        send "grant all on database ${DB_CREATE} to role ${ROLE};\n"

     expect "\$beelineStr"
        send "grant all on  URI '${NAMESERVICE}${LOCATION_ROLE}' to role ${ROLE};\n"

     expect "\$beelineStr"
        send "grant role ${ROLE} to group ${USER_GROUP};\n"

     expect "\$beelineStr"
        send "!q\n"

    expect "\$expectStr"
        send "exit\n"

END_EXPECT

}

pbrun_command_func
echo "Process Complete"