#!/bin/bash

dh_root_value=$1

cd ../../Scheduler+Agent
sed "s|dh_root_value|$dh_root_value|g" config.ini > config.ini.tmp
mv config.ini.tmp config.ini 

# Installin python and sendmail
echo -e "Installing python and sendmail... \n"
sudo apt-get install python -y
sudo apt-get install python-mysql.connector -y
sudo apt-get install sendmail -y
sudo apt-get install python-requests -y

# Start Scheduler
nohup python ./scheduler.py > scheduler_nohup.out 2>&1 &
