# codeshare
#!/usr/bin/python
import sys
import os
import shutil
#import xlrd
######Importing allscript here###################
import cleanup_delete_move_backup
import ddl_create_artifacts
import generate_md5
import load_tables
import verify_table_load
import subprocess
from os.path import dirname
import alter_describe_file

if len(sys.argv) < 3:
	print "Please enter source system name amd table type(normal or append or secure)"
	sys.exit()
sys.dont_write_bytecode = True

source_nm=str(sys.argv[1])
table_type=str(sys.argv[2])

desc_file_path=os.getcwd()+"/describe_files/"
script_path=os.getcwd()
output_artifact_dir=dirname(script_path) +"/output_artifacts/hive/"
primary_key_path=os.getcwd()+"/describe_files/primary-key/"

tab3="\t"*2

########################Location START#######################
object_name="/supplychain"
source_name="/" + source_nm + "/"
delta_table="delta/"
orc_schema_nm=source_nm

data_dictionary=source_nm + "_data_dictionary.csv" 
	
if ( table_type ==  'normal') or ( table_type ==  'append'):
	delta_schema_nm=source_nm+"_stg"
	prmary_location="/stg/sdm"
	orc_location="/data/sdm"

if ( table_type ==  'secure'):
	delta_schema_nm=source_nm+"_stg"
	delta_schema_nm=source_nm+"_secure_stg"
	prmary_location="/stgsecure/sdm"
	orc_location="/datasecure/sdm"

########################Location END#######################

########################property START########################
orc_table_property="tblproperties ('orc.compress.size'='8192');"
########################property END########################

#######################Pig variable decleration START########
#data_fu_jar="REGISTER /usr/ibmpacks/bigsheets/5.14.2/libext/datafu-1.2.0.jar"
#piggy_bank_jar="REGISTER /usr/ibmpacks/bigsheets/5.14.2/libext/piggybank.jar"

data_fu_jar="REGISTER datafu-1.2.0.jar"
piggy_bank_jar="REGISTER piggybank.jar"

storage_type="PigStorage('|')"
#######################Pig variable decleration END########

#######################Hive variable decleration START########
set_hive_dynamic_property="set hive.exec.dynamic.partition=true;"
set_hive_mapred_mode_property="set hive.mapred.mode=nonstrict;"
set_hive_dynamic_partition_property="set hive.exec.dynamic.partition.mode=nonstrict;"
set_hive_maper_memory_property="set mapreduce.map.memory.mb=8192;"
set_hive_mapreduce_java_property="set mapreduce.map.java.opts=-Xmx6144m;"
#######################Hive variable decleration END########
table_list=open(script_path + "/table_list.lst", 'r')

#################################Function Declaration START #######################################

def get_col_list(casting="NO"):
	col_list=""
	columns=open(desc_file_path + table_nm + ".describe", 'r' )
	for line in columns:
		if line !="\n":
			column_name_script=line.split(" ")[0]
			if ( "\t" in line):
				column_name_script=line.split("\t")[0].rstrip()
			
			if ( casting ==  'YES' and column_name_script.endswith("_dt")):
				col_list +=tab3+"TO_DATE("+column_name_script+ ")" +",\n"
			elif  ( casting ==  'YES' and column_name_script.endswith("_ts")):
				col_list +=tab3+"cast(("+column_name_script+ ") as timestamp) as " + column_name_script+",\n"
			else:
				col_list +=tab3+column_name_script +",\n"
	return col_list
				
def get_data_dictionary(table_name):
        file=open(os.path.join(primary_key_path, table_nm.lower() + ".primary") , 'w')
        dirname(os.getcwd())
        for line in open(os.getcwd()+ "/"+ data_dictionary):
                csv_row = str(line.split())
		# reterive table name from csv file and compare with current table name loop
		if table_name.lower() == csv_row.split(',')[0].strip("['"):
			primary_key_row=csv_row.split(',')[1]
			for primary_key in primary_key_row.split('|'):
				file.write(primary_key + "\n")

#################################Function Declaration End #######################################

# remove an already existing directory and create again###########
if os.path.exists(desc_file_path):
	shutil.rmtree(desc_file_path)
	os.makedirs(desc_file_path)
subprocess.call(['./describe_schema.sh', str(source_nm), str(table_type), str(data_dictionary)])

for table_nm in table_list:
        table_nm=table_nm.strip()
	
		
	if ( table_type ==  'normal') or ( table_type ==  'append'):
		table_directory_name=output_artifact_dir+table_nm.lower()
	if ( table_type ==  'secure'):
		table_directory_name=output_artifact_dir+table_nm.lower() + "/secure/"
		
	if not os.path.exists(table_directory_name):
               	os.makedirs(table_directory_name)
       	if not os.path.exists(output_artifact_dir):
               	os.makedirs(output_artifact_dir)
	if not os.path.exists(primary_key_path):
               	os.makedirs(primary_key_path)
		
        get_data_dictionary(table_nm.lower())
        ddl_create_artifacts.func_table_creation(table_directory_name,table_nm,desc_file_path,delta_schema_nm,orc_schema_nm,prmary_location,object_name,source_name,delta_table,orc_table_property,orc_location,output_artifact_dir,tab3,source_nm)
        cleanup_delete_move_backup.func_cleanup(table_directory_name,prmary_location,object_name,source_name,delta_table,table_nm,tab3,source_nm,delta_schema_nm,table_type)
	generate_md5.func_md5(table_directory_name,piggy_bank_jar,tab3,get_col_list,storage_type,table_nm,data_fu_jar,primary_key_path)
	load_tables.func_loads(table_directory_name,table_nm,set_hive_dynamic_property,set_hive_mapred_mode_property,set_hive_dynamic_partition_property,set_hive_maper_memory_property,set_hive_mapreduce_java_property,delta_schema_nm,tab3,get_col_list,source_nm,table_type)
        verify_table_load.func_verify_table_load(table_directory_name,prmary_location,object_name,source_name,table_nm)

print("Please check artifacts in:" + table_directory_name);
