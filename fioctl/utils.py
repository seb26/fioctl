from datetime import datetime
from functools import partialmethod
from pathlib import Path
from rich.console import Group
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TotalFileSizeColumn,
    TransferSpeedColumn,
)
from rich.table import Column, Table
from rich.text import Text
from tabulate import tabulate
from token_bucket import Limiter
from token_bucket import MemoryStorage
from tqdm import tqdm
from treelib import Node, Tree
from typing import Callable
import bitmath
import click
import concurrent.futures
import heapq
import itertools
import json
import logging
import math
import os
import re
import sys
import threading
import time
import urllib

from .config import nested_get, nested_set
from .config import config as fio_config

TRUNCATION_CUTOFF = 100

logger = logging.getLogger(__name__)

def truncate_string(string):
    if not isinstance(string, str):
        return string

    return (
        (string[:TRUNCATION_CUTOFF] + "...")
        if len(string) > TRUNCATION_CUTOFF
        else string
    )


class ListType(click.ParamType):
    name = "list"

    def convert(self, value, _param, _ctx):
        if isinstance(value, list):
            return value
        return [val.strip() for val in value.split(",")]


class UpdateType(click.ParamType):
    name = "update"

    def convert(self, value, _param, _ctx):
        update = [tuple(val.strip().split("=")) for val in value.split(",")]
        nested_update = [(column.split("."), update) for (column, update) in update]

        update = {}
        for (nested_key, val) in nested_update:
            nested_set(update, nested_key, val)

        return update


class FormatType(click.ParamType):
    name = "format"

    def __init__(self):
        self.fetch_map = {}
        super(FormatType, self).__init__()

    def convert(self, value, _param, _ctx):
        return self.formatters()[value]

    def formatters(self):
        return {
            "json": self.format_json,
            "table": self.format_table,
            "csv": self.format_csv,
            "tree": self.format_tree,
            "none": self.format_no_output,
        }
    
    def format_no_output(self, value, **kwargs):
        """No output, quiet"""
        return

    def format_json(self, value, **kwargs):
        if isinstance(value, dict):
            click.echo(self._format_json(value, **kwargs))
            return

        for val in value:
            click.echo(self._format_json(value, **kwargs))

    def _format_json(self, value, **kwargs):
        return json.dumps(value, indent=2, sort_keys=True)

    def format_tree(
        self, values, cols=["id"], root=("root", "root_id"), line_fmt=None, **kwargs
    ):
        line_fmt = line_fmt or self._tree_node
        tree = Tree()
        root_name, root = root
        tree.create_node(root_name, root)
        self._build_fetch_map(cols)

        def format_node(value, cols):
            return line_fmt((col, self._get_column(value, col)) for col in cols)

        for value in values:
            tree.create_node(
                format_node(value, cols),
                value["id"],
                parent=(value.get("parent_id") or root),
            )
        tree.show()

    def _tree_node(self, col_vals):
        return ",".join(f"{col}={val}" for col, val in col_vals)

    def format_csv(self, value, cols=["id"], **kwargs):
        if isinstance(value, dict):
            value = [value]

        self._build_fetch_map(cols)
        click.echo(",".join(cols))
        for val in value:
            click.echo(self._csv_line(val, cols))

    def _csv_line(self, value, cols):
        return ",".join(str(self._get_column(value, col)) for col in cols)

    def format_table(self, value, cols=None, **kwargs):
        tablefmt = fio_config.fetch("table", "fmt") or "psql"
        if isinstance(value, dict):
            cols = cols or value.keys()
            self._build_fetch_map(cols)
            click.echo(
                tabulate(
                    [
                        (col, self._convert(self._get_column(value, col)))
                        for col in cols
                    ],
                    headers=["attribute", "value"],
                    tablefmt=tablefmt,
                )
            )
            return

        value = list(value)
        cols = cols or list(value[0].keys())
        self._build_fetch_map(cols)
        click.echo(
            tabulate(
                self._list_table_format(value, cols), headers=cols, tablefmt=tablefmt
            )
        )

    def _get_column(self, value, column):
        column = self.fetch_map.get(column, [column])
        return truncate_string(nested_get(value, column))

    def _build_fetch_map(self, columns):
        self.fetch_map = {col: col.split(".") for col in columns}

    def _convert(self, value):
        if isinstance(value, dict):
            return self._format_json(value)
        return value

    def _list_table_format(self, l, cols):
        def tableize_row(row):
            return [self._convert(self._get_column(row, col)) for col in cols]

        return [tableize_row(row) for row in l]


class RegexType(click.ParamType):
    name = "regex"

    def convert(self, pattern, _param, _ctx):
        try:
            r = re.compile(pattern)
            search = partial(
                re.search,
                pattern
            )
            return search
        except re.error as e:
            click.ParamType.fail(f'Not a valid regex, check the string: {pattern}')


class TransferSpeedColumn(ProgressColumn):
    """Renders human readable transfer speed."""

    def render(self, task) -> Text:
        """Show data transfer speed."""
        speed = task.finished_speed or task.speed
        if speed is None:
            return Text('', style="progress.data.speed")
        data_speed_bytes = format_data_speed_bytes_sec(int(speed))
        data_speed_bits =format_data_speed_mbits_sec(int(speed))
        return Text(f"{data_speed_bytes} ({data_speed_bits})", style="progress.data.speed")


class TransferProgressItems(Progress):
    columns = Progress(
        SpinnerColumn(finished_text="✅"),
        TextColumn('[progress.description]{task.description}'),
        TextColumn('|'),
        TotalFileSizeColumn(),
        TextColumn('|'),
        BarColumn(bar_width=None),
        TextColumn('|'),
        TaskProgressColumn(),
        TextColumn('|'),
        transient = True,
    )

    def get_renderables(self):
        """Wrap it in a panel"""
        table = self.make_tasks_table(self.tasks)
        table.title = 'Files'
        table.title_style = 'bold'
        yield Panel(
            table,
        )


class TransferProgressTotal(Progress):
    def __init__(self, transfer_type: str, **kwargs):
        super(TransferProgressTotal, self).__init__(**kwargs)
        # Totals
        self.files_count_id = self.add_task('total_files_num', total=0, start=False)
        self.filesize_id = self.add_task('total_filesize', total=0)
        self.files = self._tasks[self.files_count_id]
        self.filesize = self._tasks[self.filesize_id]
        self.transfer_type = transfer_type
        if self.transfer_type == 'upload':
            self.transfer_type_display = '⬆  Uploading'
        elif self.transfer_type == 'download':
            self.transfer_type_display = '⬇  Downloading'

    
    def files_update(self, **kwargs):
        if kwargs.get('increment_total'):
            task = self._tasks[self.files_count_id]
            task.total += kwargs['increment_total']
            return task.total
        return self.update(self.files_count_id, **kwargs)
    
    def filesize_update(self, **kwargs):
        if kwargs.get('increment_total'):
            task = self._tasks[self.filesize_id]
            task.total += kwargs['increment_total']
            return task.total
        return self.update(self.filesize_id, **kwargs)

    def get_renderables(self):
        if len(self.tasks) == 0:
            return

        table_topline = Table.grid(padding=(0, 1), expand=False)
        table_topline.add_row(
            Text(f'{self.transfer_type_display}'),
            TransferSpeedColumn().render(task=self.filesize),
        )
        table_topline.row_styles = 'bold'
        table_bar = Table.grid(padding=(0, 1), expand=True)
        table_bar.add_row(
            BarColumn(bar_width=None).render(task=self.filesize),
        )
        table_detailed = Table.grid(padding=(0, 1), expand=False)
        table_detailed.add_row(
            f'{self.files.completed} of {self.files.total} file(s) -',
            f'{format_data_bytes(self.filesize.completed)} of {format_data_bytes(self.filesize.total)} -',
            'Elapsed:',
            TimeElapsedColumn().render(task=self.filesize),
            ' - Remaining:',
            TimeRemainingColumn().render(task=self.filesize),
        )
        progress_panel = Panel(
            Group(
                table_topline,
                table_detailed,
            )
        )
        yield progress_panel


def increment_task_total(progress: Progress, task_id: int, advance: int=0):
    task = progress._tasks[task_id]
    task.total += advance
    return task.total


def create_include_exclude_filter(user_re_include, user_re_exclude):
    re_include = user_re_include if user_re_include else lambda value: True
    re_exclude = user_re_exclude if user_re_exclude else lambda value: True
    def _callable(value):
        if re_include(value) and not re_exclude(value):
            return True
        return False
    return _callable


def merge_streams(*streams, key=lambda x: x["id"]):
    heap = []
    fetch = lambda stream: next(stream, None)

    def enqueue(stream, idx):
        head = fetch(stream)
        if not head:
            return
        heapq.heappush(heap, (key(head), idx, stream, head))

    for idx, stream in enumerate(streams):
        enqueue(stream, idx)

    while heap:
        _, idx, stream, result = heapq.heappop(heap)
        yield result
        enqueue(stream, idx)


def datetime_compare(first, second):
    return from_iso(first) < from_iso(second)


def from_iso(date_string):
    return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")


def exec_stream(callable, iterable, sync=lambda _: False, capacity=10, rate=10):
    """
    Executes a stream according to a defined rate limit.
    """
    limiter = Limiter(capacity, rate, MemoryStorage())
    futures = set()

    def execute(operation):
        return (operation, callable(operation))

    with concurrent.futures.ThreadPoolExecutor(max_workers=capacity) as executor:
        while True:
            if not limiter.consume("stream", 1):
                start = int(time.time())
                done, pending = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for future in done:
                    yield future.result()

                futures = pending
                if (int(time.time()) - start) < 1:
                    time.sleep(
                        1.0 / rate
                    )  # guarantee there's capacity in the rate limit at end of the loop

            operation = next(iterable, None)

            if not operation:
                done, _ = concurrent.futures.wait(futures)
                for future in done:
                    yield future.result()
                break

            if sync(operation):
                yield execute(operation)
                continue

            futures.add(executor.submit(execute, operation))


def parallelize(callable, iterable, capacity=10):
    with concurrent.futures.ThreadPoolExecutor(max_workers=capacity) as executor:
        for result in executor.map(callable, iterable):
            yield result


class Updater(object):
    def __init__(self, pbar):
        super(Updater, self).__init__()
        self.pbar = pbar

    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.pbar.total = tsize
        self.pbar.update(b * bsize - self.pbar.n)  # will also set self.n = b * bsize


def download(url, name, desc=None, position=None):
    tdm_args = dict(
        unit="B", unit_scale=True, miniters=1, desc=(desc or name), leave=False
    )
    if position:
        tdm_args["position"] = position

    with tqdm(**tdm_args) as t:
        updater = Updater(t)
        urllib.request.urlretrieve(
            url, filename=name, reporthook=updater.update_to, data=None
        )


def chunker(iterable, n):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def stream_fs(root, filter_d, filter_f):
    for directory, subdirs, files in os.walk(root):
        for d in subdirs:
            if filter_d(d):
                logger.debug(f'Filter - OK: {d}')
                yield ("d", os.path.join(directory, d))
            else:
                logger.debug(f'Filter - Skipped: {d}')
        for f in files:
            if filter_f(f):
                logger.debug(f'Filter - OK: {f}')
                yield ("f", os.path.join(directory, f))
            else:
                logger.debug(f'Filter - Skipped: {f}')

def stream_fs_pathlib(root: Path, filter_d, filter_f):
    for directory, dirs, files in root.walk():
        for d in dirs:
            if filter_d(d):
                logger.debug(f'Filter - OK: {d}')
                yield ( 'd', directory.joinpath(d) )
            else:
                logger.debug(f'Filter - Skipped: {d}')
        for f in files:
            if filter_f(f):
                logger.debug(f'Filter - OK: {f}')
                yield ( 'f', directory.joinpath(f) )
            else:
                logger.debug(f'Filter - Skipped: {f}')


def retry(callable, *args, max_retry_time_sec: int = 4, **kwargs):
    retry_time_sleep = max_retry_time_sec
    attempt = kwargs.pop("attempt", 0)
    try:
        callable(*args, **kwargs)
    except Exception as e:
        sleep_sec = min(0.5 * math.pow(2, attempt), retry_time_sleep)
        logger.warning(f"{e} - Retrying after {sleep_sec/60} min")
        time.sleep(sleep_sec)
        kwargs["attempt"] = attempt + 1
        retry(callable, *args, **kwargs)


def initialize_tqdm():
    tqdm.set_lock(threading.RLock())


class PositionTracker(object):
    def __init__(self, size):
        self.positions = range(size)
        self.used = set()
        self.lock = threading.RLock()

    def acquire(self):
        with self.lock:
            position = min(list(pos for pos in self.positions if pos not in self.used))
            self.used.add(position)
            return position

    def release(self, position):
        with self.lock:
            self.used.remove(position)


def format_data_bytes(
    bytes: int,
    pl: int = 2
) -> bitmath.Byte:
    """
    Given bytes, returns a formatted size with best units in 2 decimal places

    :param pl: decimal places to format to
    """
    return bitmath.Byte(bytes).best_prefix(system=bitmath.SI).format(
        '{value:.' + str(pl) + 'f} {unit}',
    )


def format_data_speed_bytes_sec(
    bytes: int,
    pl: int = 2,
):
    return f"{format_data_bytes(bytes, pl=pl)}/s"


def format_data_speed_mbits_sec(
    bytes: int,
    pl: int = 2,
):
    """Always Megabits (and as 'Mbps') to indicate it is an Internet-related measurement"""
    return bitmath.Byte(bytes).to_Mb().format(
        '{value:.' + str(pl) + 'f} {unit}ps',
    )