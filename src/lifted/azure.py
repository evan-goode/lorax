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

from lifted.upload import Upload, UploadError


class AzureUpload(Upload):
    """An upload to Microsoft Azure"""

    test_credentials = """
- hosts: localhost
  connection: local
  tasks:
  - name: Make sure provided credentials work and the storage account exists
    azure_rm_storageaccount_facts:
      subscription_id: "{{ subscription_id }}"
      client_id: "{{ client_id }}"
      secret: "{{ secret }}"
      tenant: "{{ tenant }}"
      resource_group: "{{ resource_group }}"
      name: "{{ storage_account_name }}"
    register: storageaccount_facts
  - name: Fail if storage account not found
    fail:
      msg: "Invalid credentials or storage account not found!"
    when: storageaccount_facts.ansible_facts.azure_storageaccounts | length < 1
"""

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
      blob: "{{ image_id }}"
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
      name: "{{ cloud_image_name }}"
      os_type: Linux
      location: "{{ location }}"
      source: "{{ source }}"
    """

    @staticmethod
    def validate_settings(settings):
        expected_settings = [
            "subscription_id",
            "client_id",
            "secret",
            "tenant",
            "resource_group",
            "storage_account_name",
            "storage_container",
            "location",
        ]
        for expected in expected_settings:
            if expected not in settings:
                raise ValueError(f"Setting {expected} expected but was not found!")
            if not settings[expected]:
                raise ValueError(f"Setting {expected} cannot be empty!")

    @staticmethod
    def get_provider():
        return "Azure"

    def _upload(self):
        self._log(f"Testing provided credentials...")
        try:
            self._run_playbook(self.test_credentials, self.settings)
        except CalledProcessError as error:
            raise UploadError(
                "Could not authenticate to Azure! Invalid credentials or missing storage account."
            ) from error
        self._log(f"Credentials look OK.")

        image_hash = hash_image(self.image_path)
        image_id = f"{self.cloud_image_name}-{image_hash}.vhd"

        storage_container = self.settings["storage_container"]
        self._log(
            f"Uploading image {self.image_path} to container {storage_container}..."
        )
        try:
            self._run_playbook(
                self.upload_image,
                {**self.settings, "image_path": self.image_path, "image_id": image_id},
            )
        except CalledProcessError as error:
            raise UploadError("Image upload failed!") from error
        self._log("Image uploaded.")

        storage_account_name = self.settings["storage_account_name"]
        host = f"{storage_account_name}.blob.core.windows.net"
        uploaded_url = f"https://{host}/{storage_container}/{image_id}"

        self._log(f"Importing image...")
        try:
            self._run_playbook(
                self.import_image,
                {
                    **self.settings,
                    "cloud_image_name": self.cloud_image_name,
                    "source": uploaded_url,
                },
            )
        except CalledProcessError as error:
            raise UploadError("Image import failed!") from error
        self._log("Image imported.")
