import click
import concurrent.futures
import math
import logging
import os
import threading
import time
from typing import List
from tqdm import tqdm
import bitmath
import requests

from . import utils

thread_local = threading.local()

logger = logging.getLogger(__name__)

class FrameioUploader(object):
    def __init__(self, asset: dict, filepath: str, position = None):
        self.asset = asset
        self.chunk_size = None
        self.chunk_urls = asset['upload_urls']
        self.chunks_num = len(self.chunk_urls)
        self.file = filepath
        self.filesize = asset["filesize"]
        self.futures = []
        self.position = position


    def _calculate_chunks(self) -> List[int]:
        """
        Calculate chunk divisions

        :return chunk_offsets: List of chunk offsets
        """
        self.chunk_size = int(math.ceil(self.filesize / self.chunks_num))
        chunk_offsets = list()
        for index in range(self.chunks_num):
            offset_amount = index * self.chunk_size
            chunk_offsets.append(offset_amount)
        return chunk_offsets


    def _get_session(self):
        if not hasattr(thread_local, "session"):
            thread_local.session = requests.Session()
        return thread_local.session


    def _smart_read_chunk(self, chunk_offset: int, is_final_chunk: bool) -> bytes:
        with open(os.path.realpath(self.file), "rb") as file:
            file.seek(chunk_offset, 0)
            if (
                is_final_chunk
            ):  # If it's the final chunk, we want to just read until the end of the file
                data = file.read()
            else:  # If it's not the final chunk, we want to ONLY read the specified chunk
                data = file.read(self.chunk_size)
            return data


    def _upload_chunk(self, task) -> int:
        url, chunk_offset, chunk_id = task
        is_final_chunk = bool( chunk_id + 1 == self.chunks_num )
        r = None
        try:
            session = self._get_session()
            chunk_data = self._smart_read_chunk(chunk_offset, is_final_chunk)
            r = utils.retry(
                session.put,
                url,
                data=chunk_data,
                headers={
                    "content-type": self.asset["filetype"],
                    "x-amz-acl": "private",
                },
                max_retry_time_sec=1920,
            )
        except Exception as e:
            logger.error(e)
            logger.debug(e, exc_info=1)
        try:
            if r:
                r.raise_for_status()
        except requests.HTTPError:
            # Retry
            self._upload_chunk(task)
        return len(chunk_data)
    

    def upload(self):
        chunk_offsets = self._calculate_chunks()
        progress_bar_args = dict(
            desc = self.asset["name"],
            leave = False,
            miniters = 0,
            total = self.asset["filesize"],
            unit = "B",
            unit_scale = True,
        )
        with tqdm(**progress_bar_args) as progress_bar:
            if self.position:
                progress_bar_args["position"] = self.position
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                start = time.start()
                for chunk_id in range(self.chunks_num):
                    url = self.chunk_urls[chunk_id]
                    chunk_offset = chunk_offsets[chunk_id]
                    task = (url, chunk_offset, chunk_id)
                    future = executor.submit(self._upload_chunk, task)
                    self.futures.append(future)
                # Keep updating the progress while we have > 0 bytes left.
                # Wait on threads to finish
                for future in concurrent.futures.as_completed(self.futures):
                    try:
                        chunk_size = future.result()
                        progress_bar.update(chunk_size)
                    except Exception as e:
                        logger.error(e)
                        logger.debug(e, exc_info=1)
                end = time.end()
                duration = tqdm.format_interval(end - start)
                speed_byte_s = bitmath.Byte(self.filesize) / self.duration
                speed_bit_s = bitmath.Byte(self.filesize).to_Bit() / self.duration
            logger.info(f'{self.filename}: Upload completed - Elapsed: {duration} - Speed: {speed_byte_s}/s ( {speed_bit_s}/s)')