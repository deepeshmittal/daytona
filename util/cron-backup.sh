#!/bin/bash

## Amazon S3 configuration required, Provide AWS cli location:
awscli=

if [ "$EUID" -ne 0 ]
  then echo "Please run as root/sudo"
  exit
fi

source config.sh

if [ -z $db_name ] || [ -z $db_user ] || [ -z $db_password ] || [ -z $db_host ] || [ -z $db_root_pass ] || [ -z $daytona_install_dir ] || [ -z $daytona_data_dir ] || [ -z $ui_admin_pass ] || [ -z $email_user ] || [ -z $email_domain ] || [ -z $smtp_server ] || [ -z $smtp_port ]; then
  echo 'one or more variables are undefined in config.sh'
  echo 'Please configure config.sh'
  echo 'For details that are unknown, enter some dummy values'
  exit 1
fi

daytona_backup_dir=${daytona_install_dir}/daytona_backup_files

./daytona_backup.sh

cd $awscli
./aws s3 cp $daytona_backup_dir s3://daytona-backup/ --recursive
