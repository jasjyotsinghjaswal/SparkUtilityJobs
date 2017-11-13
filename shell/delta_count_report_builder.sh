#!/bin/bash

set -exv

export basefolder=$1
export KEYTAB_LOCAL_PATH=XXXXXXXXXXXXXXXX
export CONN_STR=XXXXXXX@IUSER.IROOT.ADIDOM.COM
export PROJ_DIR=XXXXXXXXXXX
export BASE_PATH=${PROJ_DIR}/unloads/shell
export curr_dt=$(date +%Y-%m-%d)

check_error () {
error_msg=$1
echo $error_msg
exit 1
}



kinit $CONN_STR -k -t $KEYTAB_LOCAL_PATH ||  check_error "Cannot Authenticate via Kerberos"
#Check if input directory exist
#if $(hadoop fs -test -d  ${basefolder}/common/delta_entity_counts) ;
# then
# echo "Input Directory Exists " 
# else 
# check_error "Input DIrectory doesnt exist"
#fi
#Check if Directory for copying file to exists
if $(hadoop fs -test -f  ${basefolder}/common/delta_entity_counts_history/history_counts.dat ) ;
 then
 echo "Output Directory File exists " 
 else 
 check_error "Output Directory File doesnt exist"
fi

#Copy data from all O/P directory and create a single consolidate file and place it into current directory
hadoop fs -rm ${basefolder}/common/delta_entity_counts/*
hadoop fs -cat ${basefolder}/common/delta_ind_entity/*/* | hadoop fs -appendToFile - ${basefolder}/common/delta_entity_counts/counts.dat

#Copy Data from current delta count directory and append to history file
hadoop fs -cat ${basefolder}/common/delta_entity_counts/*  | hadoop fs -appendToFile - ${basefolder}/common/delta_entity_counts_history/history_counts.dat || check_error "Cannot append data to HDFS"

kinit $CONN_STR -k -t $KEYTAB_LOCAL_PATH ||  check_error "Cannot Authenticate via Kerberos"

##########################Copy Data to local path####################################
hadoop fs -cat ${basefolder}/common/delta_entity_counts/* |  cut -d',' -f1-5 > ${BASE_PATH}/current_entity_counts.dat || check_error "Cannot grab data from HDFS for sending email report"


################ Format data in proper format for reporting purpose #################
gawk -v curr_dt="$curr_dt" 'BEGIN{
FS=","
print "<html><body><p style='color:black'>Hi Team,</p><p style='color:black'>Please find below counts for inserts,updates and deletes for  "
print curr_dt
print "</p><table border=1 cellspacing=0 cellpadding=3>"
print "<tr>"
 print "<th style='text-align:center';'color:white';'background-color:DarkGreen'>Entity_Name</td>";
 print "<th style='text-align:center';'color:white';'background-color:DarkGreen'>Process_Name</td>";
 print "<th style='text-align:center';'color:white';'background-color:DarkGreen'>Inserts</td>";
 print "<th style='text-align:center';'color:white';'background-color:DarkGreen'>Updates</td>";
 print "<th style='text-align:center';'color:white';'background-color:DarkGreen'>Deletes</td>";
 print "</tr>"
}
 {
print "<tr>"
for(i=1;i<=NF;i++)
printf "<td style='text-align:center';'color:DarkBlue';'background-color:white'>%s</td>",$i
print "</tr>"
 }
END{
print "</TABLE></BODY></HTML>"
 }
' ${BASE_PATH}/current_entity_counts.dat > ${BASE_PATH}/current_entity_counts.html

#Send Email
mutt -e "my_hdr Content-Type: text/html" -s "Daily Delta Count Report" jasjyot_singh_jaswal@yahoo.com < ${BASE_PATH}/current_entity_counts.html || check_error "Failed to send email"


rm ${BASE_PATH}/current_entity_counts.dat
rm ${BASE_PATH}/current_entity_counts.html

