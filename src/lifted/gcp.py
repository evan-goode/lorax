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

# NOTICE: Needs this patch: https://github.com/ansible/ansible/pull/57141

import json
from subprocess import CalledProcessError

from upload import Upload, UploadError, UploadStatus

class GoogleUpload(Upload):
    """An upload to Google Cloud"""

    def __init__(self, image_name, image_path, google_variables):
        self.validate_variables(google_variables)
        super().__init__(image_name, image_path, extension="tar.gz")
        print("image_name is", self.image_name)
        self.google_variables = google_variables
        self.service_account_object = json.loads(self.google_variables["service_account_contents"])

    # TODO We would like `overwrite: no`, but Ansible throws a strange error
    # when it is set
    upload_image = """
- hosts: localhost
  connection: local
  tasks:
  - name: Upload image to Google Cloud Storage
    gcp_storage_object:
      service_account_contents: "{{ service_account_object | to_json }}"
      auth_kind: serviceaccount
      project: "{{ project }}"
      bucket: "{{ bucket }}"
      action: upload
      overwrite: yes
      src: "{{ image_path }}"
      dest: "{{ image_id }}"
    """

    # TODO
    import_image = """
- hosts: localhost
  connection: local
  tasks:
  - name: Import image
    gcp_compute_image:
      service_account_contents: "{{ service_account_object | to_json }}"
      auth_kind: serviceaccount
      project: "{{ project }}"
      name: "{{ image_name }}"
      raw_disk:
        source: "https://storage.googleapis.com/{{ bucket }}/{{ image_id }}"
      state: present
    """

    @staticmethod
    def validate_variables(variables):
        for expected in ("service_account_contents", "bucket", "project"):
            if expected not in variables:
                raise ValueError(f"Variable {expected} expected but was not found!")

    def _upload(self):
        bucket = self.google_variables["bucket"]
        self._log(f'Uploading image {self.image_path} to bucket "{bucket}"...')
        try:
            self._run_playbook(self.upload_image, {
                **self.google_variables,
                "image_path": self.image_path,
                "image_id": self.image_id,
                "service_account_object": self.service_account_object
            })
        except CalledProcessError as error:
            raise UploadError("Image upload failed!") from error
        self._log("Image uploaded.")

        self._log("Importing image...")
        try:
            self._run_playbook(self.import_image, {
                **self.google_variables,
                "image_id": self.image_id,
                "image_name": self.image_name,
                "service_account_object": self.service_account_object
            })
        except CalledProcessError as error:
            raise UploadError("Image import failed!") from error
        self._log("Image imported.")
