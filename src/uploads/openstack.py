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

class OpenStackUpload(Upload):
    """An upload to OpenStack"""

    def __init__(self, image_name, image_path, openstack_variables):
        self.validate_variables(openstack_variables)
        super().__init__(image_name, image_path, extension="qcow2")
        self.openstack_variables = openstack_variables

    upload_image = """
- hosts: localhost
  connection: local
  tasks:
  - name: Upload image to OpenStack
    os_image:
      auth:
        auth_url: "{{ auth_url }}"
        username: "{{ username }}"
        password: "{{ password }}"
        project_name: "{{ project_name }}"
        os_user_domain_name: "{{ user_domain_name }}"
        os_project_domain_name: "{{ project_domain_name }}"
      name: "{{ image_id }}"
      filename: "{{ image_path }}"
      is_public: "{{ is_public }}"
    """

    @staticmethod
    def validate_variables(variables):
        expected_variables = [
            "auth_url", "username", "password", "project_name", "user_domain_name",
            "project_domain_name", "is_public"
        ]
        for expected in expected_variables:
            if expected not in variables:
                raise ValueError(f'Variable "{expected}" expected but was not found!')

    def _upload(self):
        self._log(f"Uploading image {self.image_path} OpenStack...")
        try:
            self._run_playbook(self.upload_image, {
                **self.openstack_variables,
                "image_path": self.image_path,
                "image_id": self.image_id
            })
        except CalledProcessError as error:
            raise UploadError("Image upload failed!") from error
        self._log("Image uploaded.")
