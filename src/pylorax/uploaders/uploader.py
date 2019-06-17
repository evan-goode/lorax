#
# Copyright (C) 2018-2019 Red Hat, Inc.
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

from abc import ABC, abstractmethod
from enum import Enum, auto
import hashlib
import json
from subprocess import run, PIPE, STDOUT

import logging
log = logging.getLogger("pylorax")

CHUNK_SIZE = 65536 # 64 kibibytes

class UploadError(Exception):
    """Meant to be thrown during upload and gracefully caught"""

def hash_image(path):
    """Returns the SHA-256 checksum of a file

    :param path: path to the file to hash
    :type path: str
    :returns: the SHA-256 hexdigest
    :rtype: str
    """
    checksum = hashlib.sha256()
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(CHUNK_SIZE), b""):
            checksum.update(chunk)
    return checksum.hexdigest()

class UploaderStatus(Enum):
    """Uploaders start as WAITING, then RUNNING, then FINISHED or FAILED."""
    WAITING = auto()
    RUNNING = auto()
    FINISHED = auto()
    FAILED = auto()
    CANCELLED = auto()

class Uploader(ABC):
    """Uploads a composed image to an abstract cloud provider.
    Subclasses represent uploaders for different providers."""

    def __init__(self, image_name, image_path, settings, status_callback=None, extension="img"):
        self.validate_settings(settings)
        self.settings = settings
        self.image_name = image_name
        self.image_path = image_path
        self.image_hash = hash_image(image_path)
        self.image_id = f"{image_name}-{self.image_hash}.{extension}"
        self.status_callback = status_callback
        self.upload_log = ""
        self.error = None
        self.status = UploaderStatus.WAITING

    def set_status(self, status):
        self.status = status
        if self.status_callback:
            self.status_callback()

    @staticmethod
    @abstractmethod
    def validate_settings(settings):
        """Validates uploader settings

        :param settings: a dict of settings used by the uploader
        :type settings: dict
        :raises: ValueError if any expected settings are missing, or if any are invalid
        """

    def _log(self, message):
        """Logs something to the upload log

        :param message: the object to log
        :type message: object
        """
        self.upload_log += f"{message}\n"
        with open("/tmp/supplemental_log", "a") as suplog:
            suplog.write(f"{message}\n")
        log.info(message) # TODO

    def _run_playbook(self, playbook, variables=None):
        """Run ansible-playbook on a playbook string

        :param playbook: the full string contents of the playbook to run
        :type playbook: str
        :param variables: a dict of the variables to be passed to ansible-playbook via
                          "--extra-vars"
        :type variables: dict
        :returns: the completed process, see
                  https://docs.python.org/3/library/subprocess.html#subprocess.CompletedProcess
        :rtype: CompletedProcess
        :raises: CalledProcessError if ansible-playbook exited with a non-zero return code
        """
        result = run(["ansible-playbook", "/dev/stdin", "--extra-vars", json.dumps(variables)],
                     stdout=PIPE,
                     stderr=STDOUT,
                     input=playbook,
                     encoding='utf-8')
        self._log(result.stdout)
        result.check_returncode()
        return result

    @abstractmethod
    def _upload(self):
        """Uploads the image to the cloud

        :raises: UploadError
        """

    def upload(self):
        """Error-handling wrapper around _upload"""
        try:
            if self.status is not UploaderStatus.WAITING:
                raise UploadError("This upload has already been attempted!")
            self.set_status(UploaderStatus.RUNNING)
            self._upload()
            self.set_status(UploaderStatus.FINISHED)
        except UploadError as error:
            self._log(error)
            self.error = error
            self.set_status(UploaderStatus.FAILED)
