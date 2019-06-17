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

from datetime import datetime
from glob import glob
import logging
import multiprocessing

# We use multiprocessing processes for uploads so that we can cancel them with
# a simple SIGTERM, which bubbles down to the Ansible subprocesses.
 
from multiprocessing.dummy import Process
from multiprocessing import Pool, current_process
import os
import pickle
import signal
import time
from uuid import uuid4

from pylorax.sysutils import joinpaths
from pylorax.uploaders import UploaderStatus, DummyUploader
from pylorax.api.queue import uuid_status, uuid_image

SIMULTANEOUS_UPLOADS = 1

log = logging.getLogger("pylorax")

mpl = multiprocessing.log_to_stderr().setLevel(logging.INFO)

def get_queue_path(cfg):
    path = joinpaths(cfg.get("composer", "lib_dir"), "upload_queue")
    os.makedirs(path, exist_ok=True)
    return path

def list_upload_uuids(cfg):
    return [os.path.basename(path) for path in glob(joinpaths(get_queue_path(cfg), "*"))]

def get_upload(cfg, uuid):
    with open(joinpaths(get_queue_path(cfg), uuid), "rb") as pickle_file:
        try:
            return pickle.load(pickle_file)
        except:
            return None

def get_all_uploads(cfg):
    uploads = [get_upload(cfg, uuid) for uuid in list_upload_uuids(cfg)]
    return [upload for upload in uploads if upload]

def cancel_upload(cfg, uuid):
    get_upload(cfg, uuid).cancel()

class Upload:
    def __init__(self, cfg, uploader_type, image_name, image_path, settings):
        self.cfg = cfg
        self.uuid = str(uuid4())
        self.timestamp = datetime.now()
        self.upload_pid = None
        self.uploader = uploader_type(image_name, image_path, settings, status_callback=self.write)

    def write(self):
        with open(joinpaths(get_queue_path(self.cfg), self.uuid), "wb") as upload_file:
            pickle.dump(self, upload_file, protocol=pickle.HIGHEST_PROTOCOL)
    def execute(self):
        self.upload_pid = current_process().pid
        log.info(f"upload pid for {self.uuid} is {self.upload_pid}")
        self.uploader.upload()
    def cancel(self):
        log.info("cancelling...")
        if str(self.uploader.status) not in frozenset((str(UploaderStatus.WAITING), str(UploaderStatus.RUNNING))):
            raise RuntimeError(f"Can't cancel if status is {self.uploader.status}")
        log.info("got here...")
        if self.upload_pid:
            os.kill(self.upload_pid, signal.SIGTERM)
            log.info("sent kill")
        self.uploader.set_status(UploaderStatus.CANCELLED)

def start_upload(cfg, compose_uuid, image_name, settings):
    status = uuid_status(cfg, compose_uuid)
    uploader_type = {
        # "ami": AWSUploader,
        "vmdk": DummyUploader
    }[status["compose_type"]]
    _, image_path = uuid_image(cfg, compose_uuid)
    Upload(cfg, uploader_type, image_name, image_path, settings).write()
    return {"yeet": "deet"}

def start_upload_monitor(cfg):
    process = Process(target=monitor, args=(cfg,))
    process.daemon = True
    process.start()

def monitor(cfg):
    for upload in get_all_uploads(cfg):
        if str(upload.uploader.status) == str(UploaderStatus.RUNNING):
            log.info(f"upload {upload.uuid} abandoned, setting to failed")
            upload.uploader.set_status(UploaderStatus.FAILED)
    pool = Pool(processes=SIMULTANEOUS_UPLOADS)
    pool_uuids = set()
    while True:
        log.info("yeeted once again")
        for upload in get_all_uploads(cfg):
            log.info(f"now doing {upload.uuid}, set is {pool_uuids}, status is {upload.uploader.status}, other is {UploaderStatus.WAITING}")
            if upload.uuid not in pool_uuids and str(upload.uploader.status) == str(UploaderStatus.WAITING):
                log.info("adding...")
                pool_uuids.add(upload.uuid)
                pool.apply_async(upload.execute)
                time.sleep(10)
                log.info(f"time's up, cancelling {upload.uuid}")
                cancel_upload(cfg, upload.uuid)
        time.sleep(1)
