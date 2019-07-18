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

from ansible_runner.interface import run as ansible_run
from datetime import datetime
from enum import Enum
import hashlib
import json
import logging
from multiprocessing import current_process
import os
import signal
from subprocess import run, PIPE, STDOUT
import traceback
from uuid import uuid4

LOG = logging.getLogger("lifted")


class UploadError(Exception):
    """Meant to be thrown during upload and gracefully caught"""


class UploadStatus(Enum):
    """Uploads start as WAITING, become READY when they get an image_path,
    then RUNNING, then FINISHED, FAILED, or CANCELLED."""

    WAITING = "WAITING"
    READY = "READY"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class Upload:
    """An upload of a composed image to an abstract cloud provider.
    Subclasses represent uploads to different providers."""

    def __init__(
        self, image_name, provider, playbook_path, settings, status_callback=None
    ):
        self.validate_settings(settings)
        self.settings = settings
        self.image_name = image_name
        self.provider = provider
        self.playbook_path = playbook_path
        self.uuid = str(uuid4())
        self.creation_time = datetime.now().timestamp()
        self.upload_log = ""
        # self.error = None
        self.image_path = None
        self.upload_pid = None
        self.set_status(UploadStatus.WAITING, status_callback)

    @staticmethod
    def validate_settings(settings):
        """Validates uploader settings

        :param settings: a dict of settings used by the uploader
        :type settings: dict
        :raises: ValueError if any settings are missing or invalid
        """
        pass

    def _log(self, message):
        """Logs something to the upload log

        :param message: the object to log
        :type message: object
        """
        LOG.info(str(message))
        self.upload_log += f"{message}\n"

    def summary(self):
        """Return a dict with useful information about the upload

        :returns: upload information
        :rtype: dict
        """
        return {
            "uuid": self.uuid,
            "status": self.status.value,
            "provider": self.provider["name"],
            "image_name": self.image_name,
            "image_path": self.image_path,
            "creation_time": self.creation_time,
            # "error": str(self.error),
        }

    def set_status(self, status, status_callback=None):
        """Sets the status of the upload with an optional callback"""
        self.status = status
        if status_callback:
            status_callback(self)

    def ready(self, image_path, status_callback):
        """Provide an image_path and mark as ready to execute"""
        self.image_path = image_path
        if self.status is UploadStatus.WAITING:
            self.set_status(UploadStatus.READY, status_callback)

    def reset(self, status_callback):
        if self.is_cancellable():
            raise RuntimeError(f"Can't reset, status is {self.status.value}!")
        if not self.image_path:
            raise RuntimeError(f"Can't reset, no image supplied yet!")
        # self.error = None
        self._log("Resetting...")
        self.set_status(UploadStatus.READY, status_callback)

    def is_cancellable(self):
        """Is the upload in a cancellable state?"""
        return self.status in (
            UploadStatus.WAITING,
            UploadStatus.READY,
            UploadStatus.RUNNING,
        )

    def cancel(self, status_callback=None):
        """Cancel the upload. Sends a SIGINT to self.upload_pid"""
        if not self.is_cancellable():
            raise RuntimeError(f"Can't cancel, status is already {self.status.value}!")
        if self.upload_pid:
            os.kill(self.upload_pid, signal.SIGINT)
        self.set_status(UploadStatus.CANCELLED, status_callback)

    def execute(self, status_callback=None):
        if self.status is not UploadStatus.READY:
            raise RuntimeError("This upload is not ready!")
        self.upload_pid = current_process().pid
        self.set_status(UploadStatus.RUNNING, status_callback)
        runner = ansible_run(
            playbook=self.playbook_path,
            extravars={
                **self.settings,
                "image_name": self.image_name,
                "image_path": self.image_path,
            },
            event_handler=self._log,
        )
        if runner.status == "successful":
            self.set_status(UploadStatus.FINISHED, status_callback)
        else:
            self.set_status(UploadStatus.FAILED, status_callback)
