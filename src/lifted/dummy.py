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


class DummyUpload(Upload):
    """A dummy uploader for testing and development. Waits 30 seconds."""

    wait = """
- hosts: localhost
  connection: local
  tasks:
  - pause: seconds=30
    """

    @staticmethod
    def validate_settings(settings):
        pass

    @staticmethod
    def get_provider():
        return "Dummy"

    def _upload(self):
        self._log(f"Waiting...")
        try:
            self._run_playbook(self.wait, {"image_path": self.image_path})
        except CalledProcessError as error:
            raise UploadError("Waiting failed! (???)") from error
        self._log("Waiting finished.")
