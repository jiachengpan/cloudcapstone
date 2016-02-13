#! /bin/bash

INST_TYPE=c3.xlarge
PRICE=0.051

AWS_CONFIG=$(base64 -w0 aws_config)

cat << EOF_USER_DATA > user_data.txt
#cloud-config
repo_update: true
repo_upgrade: all

packages:
 - ntp
 - awscli

runcmd:
 - echo never > /sys/kernel/mm/transparent_hugepage/enabled
 - mkdir /home/ubuntu/.aws/
 - base64 -d $AWS_CONFIG > /home/ubuntu/.aws/config
 - chmod -R 600 /home/ubuntu/.aws/
 - chown -R ubuntu:ubuntu /home/ubuntu/.aws/

EOF_USER_DATA

USER_DATA=$(base64 -w0 user_data.txt)


cat << EOF_SPEC > spec.json
{
  "ImageId": "ami-a21529cc",
  "KeyName": "cloudcapstone",
  "InstanceType": "$INST_TYPE",
  "SecurityGroups": ["cloudcapstone"],
  "Placement": {
    "AvailabilityZone": "ap-northeast-1a"
  },
  "UserData": "$USER_DATA"
}
EOF_SPEC


aws ec2 describe-spot-price-history --instance-types $INST_TYPE --max-items 3 

aws ec2 request-spot-instances \
  --no-dry-run \
  --spot-price $PRICE \
  --instance-count 1 --type "one-time" \
  --launch-specification file://spec.json

rm -rf spec.json user_data.txt
