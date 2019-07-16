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

from operator import itemgetter
from subprocess import CalledProcessError

import boto3
from botocore.exceptions import BotoCoreError

from lifted.upload import Upload, UploadError, hash_image


class AWSUpload(Upload):
    """An upload to Amazon Web Services"""

    test_credentials = """
- hosts: localhost
  connection: local
  tasks:
  - name: Make sure provided credentials work
    aws_caller_facts:
      aws_access_key: "{{ access_key }}"
      aws_secret_key: "{{ secret_key }}"
      region: "{{ region_name }}"
"""

    ensure_ami_name_available = """
- hosts: localhost
  connection: local
  tasks:
  - name: Ensure the AMI name we want isn't taken
    ec2_ami_facts:
      aws_access_key: "{{ access_key }}"
      aws_secret_key: "{{ secret_key }}"
      region: "{{ region_name }}"
      filters:
        name: "{{ cloud_image_name }}"
    register: ami_facts
  - name: Fail if AMI name is taken
    fail:
      msg: "AMI {{ cloud_image_name }} is taken!"
    when: ami_facts.images | length > 0
"""

    ensure_vmimport_role_exists = """
- hosts: localhost
  connection: local
  tasks:
  - name: Find vmimport role
    iam_role_facts:
      aws_access_key: "{{ access_key }}"
      aws_secret_key: "{{ secret_key }}"
      region: "{{ region_name }}"
      name: vmimport
    register: role_facts
  - name: Fail if vmimport role not found
    fail:
      msg: "Role vmimport doesn't exist!"
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
      region: "{{ region_name }}"
      bucket: "{{ s3_bucket }}"
      mode: create
"""

    upload_image = """
- hosts: localhost
  connection: local
  tasks:
  - name: Upload AMI image to S3
    aws_s3:
      aws_access_key: "{{ access_key }}"
      aws_secret_key: "{{ secret_key }}"
      region: "{{ region_name }}"
      bucket: "{{ s3_bucket }}"
      src: "{{ image_path }}"
      object: "{{ image_id }}"
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
      region: "{{ region_name }}"
      name: "{{ cloud_image_name }}"
      state: present
      virtualization_type: hvm
      root_device_name: /dev/sda1
      device_mapping:
      - device_name: /dev/sda1
        snapshot_id: "{{ snapshot_id }}"
        delete_on_termination: true
"""

    @staticmethod
    def validate_settings(settings):
        expected_settings = ["access_key", "secret_key", "s3_bucket", "region_name"]
        for expected in expected_settings:
            if expected not in settings:
                raise ValueError(f"Setting {expected} expected but was not found!")
            if not settings[expected]:
                raise ValueError(f"Setting {expected} cannot be empty!")

    @staticmethod
    def get_provider():
        return "AWS"

    def _import_snapshot(self, image_id):
        """Imports an image stored on S3 as an EC2 snapshot

        :returns: a snapshot ID
        :rtype: str
        """

        # We'll tag snapshots with the image name so we don't unnecessarily import the same
        # image twice
        tag_key = "composer-image"

        ec2_client = boto3.client(
            "ec2",
            aws_access_key_id=self.settings["access_key"],
            aws_secret_access_key=self.settings["secret_key"],
            region_name=self.settings["region_name"],
        )
        response = None

        def get_snapshot(snapshot_filter):
            try:
                response = ec2_client.describe_snapshots(Filters=[snapshot_filter])
            except BotoCoreError as error:
                raise UploadError("Import snapshot failed!") from error
            snapshots = response["Snapshots"]
            if snapshots:
                # Use the most recent upload (not that there should be any duplicates)
                return max(snapshots, key=itemgetter("StartTime"))["SnapshotId"]
            return None

        # If we've already imported the snapshot, just use that
        snapshot_id = get_snapshot({"Name": f"tag:{tag_key}", "Values": [image_id]})
        if snapshot_id:
            return snapshot_id

        disk_container = {
            "Description": image_id,
            "Format": "raw",
            "UserBucket": {"S3Bucket": self.settings["s3_bucket"], "S3Key": image_id},
        }
        try:
            response = ec2_client.import_snapshot(DiskContainer=disk_container)
        except BotoCoreError as error:
            raise UploadError("Import snapshot failed!") from error

        import_task_id = response["ImportTaskId"]
        generated_description = f"Created by AWS-VMImport service for {import_task_id}"

        waiter = ec2_client.get_waiter("snapshot_completed")
        try:
            waiter.wait(
                Filters=[{"Name": "description", "Values": [generated_description]}],
                # wait for up to an hour; snapshot imports can take a while
                WaiterConfig={"Delay": 15, "MaxAttempts": 240},
            )
        except BotoCoreError as error:
            raise UploadError("Import snapshot failed!") from error

        snapshot_id = get_snapshot(
            {"Name": "description", "Values": [generated_description]}
        )

        ec2_client.create_tags(
            Resources=[snapshot_id], Tags=[{"Key": tag_key, "Value": image_id}]
        )

        return snapshot_id

    def _upload(self):
        self._log(f"Testing provided credentials...")
        try:
            self._run_playbook(self.test_credentials, self.settings)
        except CalledProcessError as error:
            raise UploadError("Could not authenticate to AWS!") from error
        self._log(f"Credentials look OK.")

        self._log(f"Ensuring AMI name {self.cloud_image_name} is available...")
        try:
            self._run_playbook(
                self.ensure_ami_name_available,
                {**self.settings, "cloud_image_name": self.cloud_image_name},
            )
        except CalledProcessError as error:
            raise UploadError(f"AMI {self.cloud_image_name} already exists!") from error
        self._log("AMI name is available.")

        self._log("Ensuring vmimport role exists...")
        try:
            self._run_playbook(self.ensure_vmimport_role_exists, self.settings)
        except CalledProcessError as error:
            raise UploadError("vmimport role does not exist!") from error
        self._log("vmimport role looks OK.")

        bucket = self.settings["s3_bucket"]
        self._log(f"Creating S3 bucket {bucket}...")
        try:
            self._run_playbook(self.create_s3_bucket, self.settings)
        except CalledProcessError as error:
            raise UploadError("Could not create S3 bucket!") from error
        self._log(f"S3 bucket {bucket} created (or already existed)")

        image_hash = hash_image(self.image_path)
        image_id = f"{self.cloud_image_name}-{image_hash}.ami"

        self._log(f"Uploading image {self.image_path} to bucket {bucket}...")
        try:
            self._run_playbook(
                self.upload_image,
                {**self.settings, "image_path": self.image_path, "image_id": image_id},
            )
        except CalledProcessError as error:
            raise UploadError("Upload to S3 failed!") from error
        self._log("Image uploaded.")

        self._log("Importing image as an EBS snapshot...")
        snapshot_id = self._import_snapshot(image_id)
        self._log(f"Snapshot successfully imported with ID {snapshot_id}.")

        self._log(f"Registering image as an AMI with name {self.cloud_image_name}...")
        try:
            self._run_playbook(
                self.register_image,
                {
                    **self.settings,
                    "cloud_image_name": self.cloud_image_name,
                    "snapshot_id": snapshot_id,
                },
            )
        except CalledProcessError as error:
            raise UploadError("Couldn't register image as an AMI!") from error
        self._log(f"Image {self.cloud_image_name} successfully registered.")
