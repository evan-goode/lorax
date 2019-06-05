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

from subprocess import CalledProcessError

from upload import Upload, UploadError, UploadStatus

class VSphereUpload(Upload):
    """An upload to VMWare vSphere"""

    def __init__(self, image_name, image_path, vsphere_variables):
        super().__init__(image_name, image_path, extension="vmdk")
        self.vsphere_variables = vsphere_variables
        if "folder" not in vsphere_variables:
            self.vsphere_variables["folder"] = "."

    upload_image = """
- hosts: localhost
  connection: local
  tasks:
  - name: Upload image to vSphere
    vsphere_copy:
      login: "{{ username }}"
      password: "{{ password }}"
      host: "{{ host }}"
      datacenter: "{{ datacenter }}"
      datastore: "{{ datastore }}"
      src: "{{ image_path }}"
      path: "{{ folder }}/{{ image_id }}"
    """

    @staticmethod
    def validate_variables(variables):
        expected_variables = [
            "datacenter", "datastore", "host", "username", "password"
        ]
        for expected in expected_variables:
            if expected not in variables:
                raise ValueError(f'Variable "{expected}" expected but was not found!')

    def _upload(self):
        datastore = self.vsphere_variables["datastore"]
        self._log(f"Uploading image {self.image_path} to datastore {datastore}...")
        try:
            self._run_playbook(self.upload_image, {
                **self.vsphere_variables,
                "image_path": self.image_path,
                "image_id": self.image_id
            })
        except CalledProcessError as error:
            raise UploadError("Image upload failed!") from error
        self._log("Image uploaded.")
