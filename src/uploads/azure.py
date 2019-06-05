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

class AzureUpload(Upload):
    """An upload to Microsoft Azure"""

    def __init__(self, vm_name, image_path, azure_variables):
        super().__init__(vm_name, image_path, extension="vhd")
        self.azure_variables = azure_variables

    upload_image = """
- hosts: localhost
  connection: local
  tasks:
  - name: Upload image to Azure
    azure_rm_storageblob:
      subscription_id: "{{ subscription_id }}"
      client_id: "{{ client_id }}"
      secret: "{{ secret }}"
      tenant: "{{ tenant }}"
      resource_group: "{{ resource_group }}"
      storage_account_name: "{{ storage_account_name }}"
      container: "{{ storage_container }}"
      src: "{{ image_path }}"
      blob: "{{ image_name }}"
      blob_type: page
      force: no
    """

    import_image = """
- hosts: localhost
  connection: local
  tasks:
  - name: Import image
    azure_rm_image:
      subscription_id: "{{ subscription_id }}"
      client_id: "{{ client_id }}"
      secret: "{{ secret }}"
      tenant: "{{ tenant }}"
      resource_group: "{{ resource_group }}"
      name: "{{ image_name }}"
      os_type: Linux
      location: "{{ location }}"
      source: "{{ source }}"
    """

    @staticmethod
    def validate_variables(variables):
        expected_variables = [
            "subscription_id", "client_id", "secret", "tenant", "resource_group",
            "storage_account_name", "storage_container", "location"
        ]
        for expected in expected_variables:
            if expected not in variables:
                raise ValueError(f'Variable "{expected}" expected but was not found!')

    def _upload(self):
        storage_container = self.azure_variables["storage_container"]
        self._log(f"Uploading image {self.image_path} to container {storage_container}...")
        try:
            self._run_playbook(self.upload_image, {
                **self.azure_variables,
                "image_path": self.image_path,
                "image_name": self.image_name
            })
        except CalledProcessError as error:
            raise UploadError("Image upload failed!") from error
        self._log("Image uploaded.")

        storage_account_name = self.azure_variables["storage_account_name"]
        host = f"{storage_account_name}.blob.core.windows.net"
        uploaded_url = f"https://{host}/{storage_container}/{self.image_name}"

        self._log(f"Importing image...")
        try:
            self._run_playbook(self.import_image, {
                **self.azure_variables,
                "image_name": self.image_name,
                "source": uploaded_url
            })
        except CalledProcessError as error:
            raise UploadError("Image import failed!") from error
        self._log("Image imported.")
