#!/usr/bin/env python

import sys
import subprocess
import datetime

msg = "number of args must be 4 , source system {0}, batch id {1},usergroup {2} and layer {3}"

if len(sys.argv) <= 4:
 print msg
 exit()

source = sys.argv[1]
batchuser = sys.argv[2]
usergroup = sys.argv[3]
layer = sys.argv[4]
source_pref = sys.argv[5]
owner = batchuser + ':' + usergroup
print (owner)

landing_dir ="/bigdatahdfs/landing/" + source
publish_dir ="/bigdatahdfs/"+layer+"/"+ source
raw_dir ="/bigdatahdfs/"+layer+"/raw/" + source
hdfs_home = "/user/" + batchuser
project_dir ="/bigdatahdfs/"+layer+"/" + source






def run_cmd(args_list):
        """
        run linux commands
        """
        # import subprocess
        print('Running system command: {0}'.format(' '.join(args_list)))
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        s_return =  proc.returncode
        return s_return, s_output, s_err


(ret, out, err)= run_cmd(['kinit', 'syshdfschew', '-kt', '/etc/security/keytabs/hdfs.headless.keytab'])
print "-----------------------------------------------------"



def functional_layer():
       print ("Creating home directory in %s:" % hdfs_home)
       (ret, out, err)= run_cmd(['hdfs', 'dfs', '-mkdir', '-p', hdfs_home ])
       print ("Creating directory in %s:" % landing_dir)
       (ret, out, err)= run_cmd(['hdfs', 'dfs', '-mkdir', '-p', landing_dir ])
       print ("Creating directory in %s:" % project_dir)
       (ret, out, err)= run_cmd(['hdfs', 'dfs', '-mkdir', '-p', project_dir ])

       print "-----------------------------------------------------"
       print ("Changing permissions of directory in %s:" % landing_dir)
       (ret, out, err)= run_cmd(['hdfs', 'dfs', '-chown', '-R', owner , landing_dir ])
       print ("Changing permissions of directory in %s:" % project_dir)
       (ret, out, err)= run_cmd(['hdfs', 'dfs', '-chown', '-R', owner , project_dir ])

       print "-----------------------------------------------------"
       print ("Changing ownership of directory in %s:" % landing_dir)
       (ret, out, err)= run_cmd(['hdfs', 'dfs', '-chmod', '-R', '750' , landing_dir ])
       print ("Changing ownership of directory in %s:" % project_dir)
       (ret, out, err)= run_cmd(['hdfs', 'dfs', '-chmod', '-R', '750' , project_dir ])

def datalake_layer():

      print ("Creating home directory in %s:" % hdfs_home)
      (ret, out, err)= run_cmd(['hdfs', 'dfs', '-mkdir', '-p', hdfs_home ])
      print ("Creating directory in %s:" % landing_dir)
      (ret, out, err)= run_cmd(['hdfs', 'dfs', '-mkdir', '-p', landing_dir ])
      print ("Creating directory in %s:" % publish_dir)
      (ret, out, err)= run_cmd(['hdfs', 'dfs', '-mkdir', '-p', publish_dir ])
      print ("Creating directory in %s:" % raw_dir)
      (ret, out, err)= run_cmd(['hdfs', 'dfs', '-mkdir', '-p', raw_dir ])

      print "-----------------------------------------------------"
      print ("Changing permissions of directory in %s:" % landing_dir)
      (ret, out, err)= run_cmd(['hdfs', 'dfs', '-chown', '-R', owner , landing_dir ])
      print ("Changing permissions of directory in %s:" % publish_dir)
      (ret, out, err)= run_cmd(['hdfs', 'dfs', '-chown', '-R', owner , publish_dir ])
      print ("Changing permissions of directory in %s:" % raw_dir)
      (ret, out, err)= run_cmd(['hdfs', 'dfs', '-chown', '-R', owner , raw_dir ])

      print "-----------------------------------------------------"
      print ("Changing ownership of directory in %s:" % landing_dir)
      (ret, out, err)= run_cmd(['hdfs', 'dfs', '-chmod', '-R', '750' , landing_dir ])
      print ("Changing ownership of directory in %s:" % publish_dir)
      (ret, out, err)= run_cmd(['hdfs', 'dfs', '-chmod', '-R', '750' , publish_dir ])
      print ("Changing ownership of directory in %s:" % raw_dir)
      (ret, out, err)= run_cmd(['hdfs', 'dfs', '-chmod', '-R', '750' , raw_dir ])

def createEncryptionDatalake():
      print ("Creating encryption for %s:" % source_pref)
      (ret, out, err)= run_cmd(['hadoop', 'key', 'create', source_pref ])
      (ret, out, err)= run_cmd(['hdfs', 'crypto', '-createZone' , '-keyName' , source_pref , '-path' , publish_dir ])
      (ret, out, err)= run_cmd(['hdfs', 'crypto', '-createZone' , '-keyName' , source_pref , '-path' , raw_dir ])
      (ret, out, err)= run_cmd(['hdfs' , 'crypto' , '-listZones' , 'grep' , '|' , source_pref ])
	 
def createEncryptionFunctional():
      print ("Creating encryption for %s:" % source_pref)
      (ret, out, err)= run_cmd(['hadoop', 'key', 'create', source_pref ])
      (ret, out, err)= run_cmd(['hdfs', 'crypto', '-createZone' , '-keyName' , source_pref , '-path' , project_dir ])
      
	 
	 


if layer == 'datalake':
   datalake_layer()
   createEncryptionDatalake()
elif layer == 'project':
   functional_layer()
   createEncryptionFunctional();
else:
 print("*******************************************")
 print("The selected layer does not exist in Hadoop")

print "*************************************"


