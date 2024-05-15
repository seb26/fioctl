from pathlib import Path
from rich.progress import Progress, TaskID
from typing import List, Callable
import concurrent.futures
import logging
import math
import os
import requests
import threading
import time

from . import utils

thread_local = threading.local()

logger = logging.getLogger(__name__)

class FrameioUploader(object):
    def __init__(self, asset: dict, filepath: str | Path, position = None, progress_callback: Callable = None):
        self.asset = asset
        self.chunk_size = None
        self.chunk_urls = asset['upload_urls']
        self.chunks_num = len(self.chunk_urls)
        self.file = filepath
        self.filename = asset['name']
        self.filesize = asset['filesize']
        self.futures = []
        self.position = position
        self.progress_callback = progress_callback


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
        if not hasattr(thread_local, 'session'):
            thread_local.session = requests.Session()
        return thread_local.session


    def _smart_read_chunk(self, chunk_offset: int, is_final_chunk: bool) -> bytes:
        with open(os.path.realpath(self.file), 'rb') as file:
            file.seek(chunk_offset, 0)
            if is_final_chunk:
                # If it's the final chunk, we want to just read until the end of the file
                data = file.read()
            else:
                # If it's not the final chunk, we want to ONLY read the specified chunk
                data = file.read(self.chunk_size)
            return data


    def _upload_chunk(self, task) -> int:
        url, chunk_offset, chunk_id = task
        is_final_chunk = bool( chunk_id + 1 == self.chunks_num )
        r = None
        session = self._get_session()
        chunk_data = self._smart_read_chunk(chunk_offset, is_final_chunk)
        r = utils.retry(
            session.put,
            url,
            data = chunk_data,
            headers = {
                'content-type': self.asset['filetype'],
                'x-amz-acl': 'private',
            },
            max_retry_time_sec = 1920,
        )
        try:
            if r:
                r.raise_for_status()
        except requests.HTTPError:
            # Retry
            self._upload_chunk(task)
        return len(chunk_data)
    

    def upload(self) -> bool:
        logger.debug(f'Start upload: {self.filename} ({self.asset['id']})')
        start = time.time()
        chunk_offsets = self._calculate_chunks()
        first_chunk_completed = False
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Create a task for each chunk
            for chunk_id in range(self.chunks_num):
                url = self.chunk_urls[chunk_id]
                chunk_offset = chunk_offsets[chunk_id]
                task = (url, chunk_offset, chunk_id)
                future = executor.submit(self._upload_chunk, task)
                self.futures.append(future)
            # Wait on threads to finish
            for future in concurrent.futures.as_completed(self.futures):
                try:
                    if first_chunk_completed is False:
                        self.progress_callback(start=True)
                        first_chunk_completed = True
                    chunk_size = future.result()
                    self.progress_callback(chunk_size)
                except Exception as e:
                    logger.error(e)
                    logger.debug(e, exc_info=1)
                    return False
        end = time.time()
        speed_byte_s = utils.format_data_speed_bytes_sec( self.filesize / ( end - start ) )
        speed_bit_s = utils.format_data_speed_mbits_sec( self.filesize / ( end - start ) )
        logger.info(f'{self.filename}: Upload completed - Elapsed: - Speed: {speed_byte_s} ({speed_bit_s})')
        return True