#!/bin/bash

# This shell script will create the service and then initialize it (if needed)

 # exit on errors
 set -o errexit

 # exit on errors in pipe
 set -o pipefail

# Create the service
cf create-service spark ibm.SparkService.PayGoPersonal my-spark-service

# # Did the create-service command return a return code of 0 (success)?
# if [ "$?" == "0" ]; then
#   echo ""
# 	echo "The Spark service already existed or was created sucessfully"
# else
#   echo ""
# 	echo "Creation of the Spark service failed"
#   exit 1
# fi
