#
# Copyright (C) 2019 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import json
import operator
from subprocess import CalledProcessError

import boto3
from botocore.exceptions import BotoCoreError

from deployment import Deployment, DeploymentError

class AWSDeployment(Deployment):
    """A deployment to Amazon Web Services"""

    def __init__(self, vm_name, image_path, aws_variables):
        super().__init__(vm_name, image_path, extension="ami")
        self.validate_variables(aws_variables)
        self.aws_variables = aws_variables

    ensure_vmimport_role_exists = """
- hosts: localhost
  connection: local
  tasks:
  - name: Ensure vmimport role exists
    iam_role_facts:
      aws_access_key: "{{ access_key }}"
      aws_secret_key: "{{ secret_key }}"
      name: vmimport
    register: role_facts
  - fail:
      msg: "Role vmimport doesn't exist"
    when: role_facts.iam_roles | length < 1
"""

    create_s3_bucket = """
- hosts: localhost
  connection: local
  tasks:
  - name: Create the S3 bucket if it doesn't exist
    aws_s3:
      aws_access_key: "{{ access_key }}"
      aws_secret_key: "{{ secret_key }}"
      bucket: "{{ s3_bucket }}"
      mode: create
"""

    upload = """
- hosts: localhost
  connection: local
  tasks:
  - name: Upload AMI image to S3
    aws_s3:
      aws_access_key: "{{ access_key }}"
      aws_secret_key: "{{ secret_key }}"
      bucket: "{{ s3_bucket }}"
      src: "{{ image_path }}"
      object: "{{ image_name }}"
      mode: put
      overwrite: never
"""

    register_image = """
- hosts: localhost
  connection: local
  tasks:
  - name: Register snapshot as an EC2 image
    ec2_ami:
      aws_access_key: "{{ access_key }}"
      aws_secret_key: "{{ secret_key }}"
      name: "{{ vm_name }}-ami"
      state: present
      virtualization_type: hvm
      root_device_name: /dev/sda1
      device_mapping:
      - device_name: /dev/sda1
        snapshot_id: "{{ snapshot_id }}"
        delete_on_termination: true
"""

    create_virtual_machine = """
- hosts: localhost
  connection: local
  tasks:
  - name: Get AMI ID
    ec2_ami_facts:
      aws_access_key: "{{ access_key }}"
      aws_secret_key: "{{ secret_key }}"
      filters:
        name: "{{ vm_name }}-ami"
    register: ami_results
  - name: Create EC2 virtual machine
    ec2_instance:
      aws_access_key: "{{ access_key }}"
      aws_secret_key: "{{ secret_key }}"
      name: "{{ vm_name }}"
      image_id: "{{ ami_results.images[0].image_id }}"
      instance_type: "{{ vm_type }}"
      security_groups: "{{ security_groups }}"
      state: present
"""

    @staticmethod
    def validate_variables(variables):
        for expected in ["access_key", "secret_key", "s3_bucket", "region_name"]:
            if expected not in variables:
                raise ValueError(f'Variable "{expected}" expected but was not found!')

    def _import_snapshot(self):
        """Imports an image stored on S3 as an EC2 snapshot

        :returns: a snapshot ID
        :rtype: str
        """

        # We'll tag snapshots with the image name so we don't unnecessarily import the same
        # image twice
        tag_key = "composer-deployment"

        ec2_client = boto3.client(
            "ec2",
            aws_access_key_id=self.aws_variables["access_key"],
            aws_secret_access_key=self.aws_variables["secret_key"],
            region_name=self.aws_variables["region_name"]
        )
        response = None

        def get_snapshot(snapshot_filter):
            try:
                response = ec2_client.describe_snapshots(Filters=[snapshot_filter])
            except BotoCoreError as error:
                raise DeploymentError("Import snapshot failed") from error
            snapshots = response["Snapshots"]
            if snapshots:
                # Use the most recent upload (not that there should be any duplicates)
                return max(snapshots, key=operator.itemgetter("StartTime"))["SnapshotId"]
            return None

        # If we've already imported the snapshot, just use that
        snapshot_id = get_snapshot({
            "Name": f"tag:{tag_key}",
            "Values": [self.image_name]
        })
        if snapshot_id:
            return snapshot_id

        disk_container = {
            "Description": self.image_name,
            "Format": "raw",
            "UserBucket": {
                "S3Bucket": self.aws_variables["s3_bucket"],
                "S3Key": self.image_name
            }
        }
        try:
            response = ec2_client.import_snapshot(DiskContainer=disk_container)
        except BotoCoreError as error:
            raise DeploymentError("Import snapshot failed!") from error

        import_task_id = response["ImportTaskId"]
        generated_description = f"Created by AWS-VMImport service for {import_task_id}"

        waiter = ec2_client.get_waiter("snapshot_completed")
        try:
            waiter.wait(Filters=[{
                "Name": "description",
                "Values": [generated_description]
            }])
        except BotoCoreError as error:
            raise DeploymentError("Import snapshot failed!") from error

        snapshot_id = get_snapshot({
            "Name": "description",
            "Values": [generated_description]
        })

        ec2_client.create_tags(Resources=[snapshot_id], Tags=[{
            "Key": "composer-deployment",
            "Value": self.image_name
        }])

        return snapshot_id

    def _deploy(self):
        image_variables = {
            "image_path": self.image_path,
            "image_name": self.image_name
        }

        self._log("Ensuring vmimport role exists...")
        try:
            self._run_playbook(self.ensure_vmimport_role_exists, self.aws_variables)
        except CalledProcessError as error:
            raise DeploymentError("vmimport role does not exist!") from error
        self._log("vmimport role looks OK.")

        bucket = self.aws_variables["s3_bucket"]
        self._log(f"Creating S3 bucket {bucket}...")
        try:
            self._run_playbook(self.create_s3_bucket, self.aws_variables)
        except CalledProcessError as error:
            raise DeploymentError("Could not create S3 bucket!") from error
        self._log(f"S3 bucket {bucket} created (or already existed)")

        self._log(f"Uploading image {self.image_path} to bucket {bucket}...")
        try:
            self._run_playbook(self.upload, {**self.aws_variables, **image_variables})
        except CalledProcessError as error:
            raise DeploymentError("Upload to S3 failed!") from error
        self._log("Image uploaded.")

        self._log("Importing image as an EBS snapshot...")
        snapshot_id = self._import_snapshot()
        self._log(f"Snapshot successfully imported. ID is {snapshot_id}.")

        self._log(f"Registering image as an AMI with name {self.vm_name}...")
        try:
            self._run_playbook(self.register_image, {
                **self.aws_variables,
                "image_name": self.image_name,
                "vm_name": self.vm_name,
                "snapshot_id": snapshot_id
            })
        except CalledProcessError as error:
            raise DeploymentError("Couldn't register image as an AMI!") from error
        self._log(f"Image {self.vm_name} successfully registered.")

        self._log(f"Creating EC2 virtual machine {self.vm_name}...")
        try:
            self._run_playbook(self.create_virtual_machine, {
                **self.aws_variables,
                "vm_name": self.vm_name,
                "security_groups": json.dumps(self.aws_variables["security_groups"])
            })
        except CalledProcessError as error:
            raise DeploymentError("Couldn't create virtual machine!") from error
        self._log(f"Virtual machine created successfully.")
