from collections import Counter
from functools import partial
from pathlib import Path
from typing import Callable, List, Generator, Iterable, Tuple
import click
import logging
import mimetypes
import os

from rich.live import Live
from rich.console import Group
from rich.progress import Progress

from . import utils
from .fio import fio_client, stream_endpoint
from .config import column_default
from .uploader import FrameioUploader

logger = logging.getLogger(__name__)

DEFAULT_COLS = column_default("assets", "id,name,type,project_id,filesize,private")

PROXY_TABLE = {
    ("high", "stream"): ["h264_2160", "h264_1080_best"],
    ("medium", "stream"): ["h264_720"],
    ("low", "stream"): ["h264_540", "h264_360"],
    ("high", "image"): ["image_high"],
    ("medium", "image"): ["image_full"],
    ("low", "image"): ["image_small"],
}

PROXY_CASCADE = ["high", "medium", "low"]

class QueuedAsset(object):
    pass

class QueuedAssetUpload(QueuedAsset):
    def __init__(
        self,
        destination_id: str,
        filepath: str | Path,
        origin_path: str | Path,
        type: str,
        asset: dict = None,
    ):
        self.asset = asset
        self.destination_id = destination_id
        self.filepath = filepath
        self.origin_path = origin_path
        self.type = type
        if self.type == 'f':
            self.filesize = filepath.stat().st_size

    def __str__(self):
        return self.filepath

class UploadQueue(object):
    def __init__(self):
        self.queue = []
    
    def add(self, item: QueuedAsset):
        self.queue.append(item)
        logger.debug(f'Added item to queue: {item.filepath}')

    def remove(self, item: QueuedAsset):
        self.queue.remove(item)

    @property
    def all(self) -> Generator:
        return ( i for i in sorted(self.queue, key=lambda item: item.filepath) )


@click.group()
def assets():
    """Asset related commands"""


@assets.command(help="List all assets you're a member of")
@click.argument("parent_id")
@click.option("--format", type=utils.FormatType(), default="table")
@click.option("--columns", type=utils.ListType(), default=DEFAULT_COLS)
def list(parent_id, format, columns):
    assets = stream_endpoint(f"/assets/{parent_id}/children")

    format(assets, cols=columns)


@assets.command(help="Views a specific asset")
@click.argument("asset_id")
@click.option("--format", type=utils.FormatType(), default="table")
@click.option("--columns", type=utils.ListType(), default=DEFAULT_COLS)
def get(asset_id, format, columns):
    assets = fio_client()._api_call("get", f"/assets/{asset_id}")

    format(assets, cols=columns)


@assets.command(help="Shows the file tree within a given asset")
@click.argument("asset_id")
@click.option("--format", type=utils.FormatType(), default="tree")
@click.option("--columns", type=utils.ListType(), default=["id", "type", "name"])
def traverse(asset_id, format, columns):
    def line_fmt(col_vals):
        attrs = ", ".join(f"{col}: {val}" for col, val in col_vals)
        return f"Asset[{attrs}]"

    format(
        (
            asset
            for _, asset in remote_folder_stream(fio_client(), asset_id, "/", recurse_vs=True)
        ),
        cols=columns,
        root=(f"Asset[id: {asset_id}]", asset_id),
        line_fmt=line_fmt,
    )


@assets.command(help="Restores a deleted asset")
@click.argument("asset_id")
@click.option("--format", type=utils.FormatType(), default="table")
@click.option("--columns", type=utils.ListType(), default=DEFAULT_COLS)
def restore(asset_id, format, columns):
    asset = fio_client()._api_call("put", f"/assets/{asset_id}/restore")
    format(asset, cols=columns)


@assets.command(help="Moves an asset to a new parent")
@click.argument("asset_id")
@click.argument("parent_id")
@click.option("--format", type=utils.FormatType(), default="table")
@click.option("--columns", type=utils.ListType(), default=DEFAULT_COLS)
def mv(asset_id, parent_id, format, columns):
    asset = fio_client()._api_call(
        "post", f"/assets/{parent_id}/move", {"id": asset_id}
    )

    format(asset, cols=columns)


@assets.command(help="Copies an asset a new parent")
@click.argument("asset_id")
@click.argument("parent_id")
@click.option("--format", type=utils.FormatType(), default="table")
@click.option("--columns", type=utils.ListType(), default=DEFAULT_COLS)
def cp(asset_id, parent_id, format, columns):
    asset = fio_client()._api_call(
        "post", f"/assets/{parent_id}/copy", {"id": asset_id}
    )

    format(asset, cols=columns)


@assets.command(help="Deletes an asset")
@click.argument("asset_id")
@click.option("--format", type=utils.FormatType(), default="table")
@click.option("--columns", type=utils.ListType(), default=DEFAULT_COLS)
def rm(asset_id, format, columns):
    columns.append("deleted_at")
    asset = fio_client()._api_call("delete", f"/assets/{asset_id}")

    format(asset, cols=columns)


@assets.command(help="Updates the workflow status on an asset")
@click.argument("asset_id")
@click.argument(
    "label", type=click.Choice(["none", "in_progress", "needs_review", "approved"])
)
@click.option("--format", type=utils.FormatType(), default="table")
@click.option("--columns", type=utils.ListType(), default=DEFAULT_COLS)
def label(asset_id, label, format, columns):
    columns.append("label")
    asset = fio_client()._api_call(
        "post", f"/assets/{asset_id}/label", {"label": label}
    )

    format(asset, cols=columns)


@assets.command(help="Versions an asset onto another asset")
@click.argument("asset_id")
@click.argument("prev_asset_id")
@click.option("--format", type=utils.FormatType(), default="table")
@click.option("--columns", type=utils.ListType(), default=DEFAULT_COLS)
def version(asset_id, prev_asset_id, format, columns):
    asset = fio_client()._api_call(
        "post", f"/assets/{prev_asset_id}/version", {"next_asset_id": asset_id}
    )

    format(asset, cols=columns)


@assets.command(help="Unversions an asset")
@click.argument("asset_id")
@click.option("--format", type=utils.FormatType(), default="table")
@click.option("--columns", type=utils.ListType(), default=DEFAULT_COLS)
def unversion(asset_id, format, columns):
    asset = fio_client()._api_call("delete", f"/assets/{asset_id}/unversion")

    format(asset, cols=columns)


@assets.command(help="Uploads an asset with a given file")
@click.pass_context
@click.argument("parent_id")
@click.argument("disk_items", type=click.Path(exists=True, path_type=Path), nargs=-1, required=True)
@click.option("--values", type=utils.UpdateType())
@click.option("--format", type=utils.FormatType(), default="none")
@click.option("--columns", type=utils.ListType(), default=DEFAULT_COLS)
@click.option(
    "--contents-only",
    is_flag=True,
    help="When using --recursive with a folder on disk, upload contents directly into the specified remote folder (`parent_id`) without creating a subfolder",
)
@click.option(
    "--include-files",
    type=utils.RegexType(),
    help="Regex pattern to include disk files from being uploaded. Applies only to files found beneath the disk items.)",
)
@click.option(
    "--include-folders",
    type=utils.RegexType(),
    help="Regex pattern to include disk folders from being uploaded. Applies only to folders found beneath the disk items.)",
)
@click.option(
    "--exclude-files",
    type=utils.RegexType(),
    help="Regex pattern to exclude disk files from being uploaded. Applies only to files found beneath the disk items.)",
)
@click.option(
    "--exclude-folders",
    type=utils.RegexType(),
    help="Regex pattern to exclude disk folders from being uploaded. Applies only to folders found beneath the disk items.)",
)
def upload(
    ctx,
    parent_id,
    disk_items: Iterable[Path],
    values,
    format,
    columns,
    contents_only,
    include_files,
    include_folders,
    exclude_files,
    exclude_folders
): 
    def handle_result(queued_asset, remote_asset) -> dict:
        # Remove from queue
        queue.remove(queued_asset)
        # Mark that we have completed 1 item
        progress_total.files_update(advance=1)
        return remote_asset
    
    def on_filesize_uploaded(task, advance: int = 0):
        progress_per_file.update(task, advance=advance)
        progress_total.filesize_update(advance=advance)

    if values:
        click.ParamType.fail('Specifying `values` is not yet implemented for `fioctl assets upload`')

    # FIO api client
    client = fio_client()

    # Blank filters - to be overridden by user filters
    filter_d = lambda pass_all: True
    filter_f = lambda pass_all: True

    # Store files/folders to upload
    queue = UploadQueue()

    # Progress bar from `rich.progress`
    # PER FILE
    progress_per_file = utils.TransferProgressItems(expand=True)
    progress_total = utils.TransferProgressTotal('upload')

    live_group = Group(
        progress_per_file,
        progress_total,
    )

    with Live(live_group):
        logger.debug(f"Disk items: {disk_items}")

        for disk_item in disk_items:
            if disk_item.is_dir():
                if include_folders or exclude_folders:
                    filter_d = utils.create_include_exclude_filter(include_folders, exclude_folders)
                if include_files or exclude_files:
                    filter_f = utils.create_include_exclude_filter(include_files, exclude_files)
                if contents_only:
                    # Disk contents will go directly into the remote parent folder without any subfolder
                    destination_parent_id = parent_id
                else:
                    # Create a parent folder on the remote side, that takes the name of the input disk folder
                    folder_asset = create_asset(
                        client, parent_id, {"type": "folder", "name": disk_item.name}
                    )
                    destination_parent_id = folder_asset['id']
                # Look at the disk tree and queue items
                stream = utils.stream_fs_pathlib(
                    disk_item,
                    filter_d = filter_d,
                    filter_f = filter_f,
                )
                for type, filepath in stream:
                    # Add files and folders to the queue.
                    # Folders: just the path is stored, creation of remote assets does not take place until
                    # the queue is actually processed. 
                    queued_item = QueuedAssetUpload(
                        destination_id = destination_parent_id,
                        filepath = filepath,
                        origin_path = disk_item,
                        type = type,
                    )
                    queue.add(queued_item)
                    if type == 'f':
                        # Don't increment folders
                        progress_total.files_update(increment_total=1)
                        progress_total.filesize_update(increment_total=queued_item.filesize)

            elif disk_item.is_file():
                queued_item = QueuedAssetUpload(
                    destination_id = parent_id, # This file will go directly into the specified remote folder 
                    filepath = disk_item,
                    origin_path = disk_item.parent, # Its origin path is the parent folder
                    type = 'f',
                )
                queue.add(queued_item)
                progress_total.files_update(increment_total=1)
                progress_total.filesize_update(increment_total=queued_item.filesize)
        
        if len(queue.queue) == 0:
            ctx.exit()

        # Go through the queue
        upload = upload_handler(
            client,
            queue = queue,
            queued_items = queue.all,
            progress = progress_per_file,
            progress_callback = on_filesize_uploaded,
        )
        results = [ handle_result(*u) for u in upload ]
        format(
            results,
            cols = ["source", "outcome"] + columns,
        )


@assets.command(help="Downloads an asset")
@click.argument("asset_id")
@click.argument("destination", type=click.Path(), required=False)
@click.option(
    "--proxy",
    type=click.Choice(
        [
            "original",
            "h264_360",
            "h264_540",
            "h264_720",
            "h264_1080_best",
            "h264_2160",
            "high",
            "medium",
            "low",
        ]
    ),
    default="original",
)
@click.option(
    "--recursive",
    is_flag=True,
    help="Downloads the children of a given asset and all of their (transitive) children",
)
@click.option("--format", type=utils.FormatType(), default="table")
def download(asset_id, destination, proxy, recursive, format):
    client = fio_client()
    if recursive:
        format(
            download_stream(client, asset_id, destination, proxy),
            cols=["source_id", "destination"],
        )
        return

    asset = client._api_call("get", f"/assets/{asset_id}")
    if asset['type'] == 'folder' and not recursive:
        click.echo("This item is a folder. To download a folder and its contents, use --recursive", err=True)
        return
    proxy, url = get_proxy(asset, proxy)
    destination = destination or filename(asset["name"], proxy)
    click.echo(f"Downloading to {destination}...")
    utils.download(url, destination, desc=asset["name"])
    click.echo("Finished download")


@assets.command(help="Updates an asset")
@click.argument("asset_id")
@click.option("--values", type=utils.UpdateType())
@click.option("--format", type=utils.FormatType(), default="table")
@click.option("--columns", type=utils.ListType(), default=DEFAULT_COLS)
def set(asset_id, values, format, columns):
    assets = fio_client()._api_call("put", f"/assets/{asset_id}", values)
    format(assets, cols=columns)


@assets.command(help="Updates an asset")
@click.argument("parent_id")
@click.option("--values", type=utils.UpdateType())
@click.option("--format", type=utils.FormatType(), default="table")
@click.option("--columns", type=utils.ListType(), default=DEFAULT_COLS)
def create(parent_id, values, format, columns):
    assets = fio_client()._api_call("post", f"/assets/{parent_id}/children", values)
    format(assets, cols=columns)


def filename(name, proxy, path=None):
    def proxy_ext(proxy):
        if "image" in proxy:
            return "jpeg"
        return "mp4"

    name, ext = os.path.splitext(name)

    default_name = (
        f"{name}{ext}" if proxy == "original" else f"{name}.{proxy}.{proxy_ext(proxy)}"
    )
    return os.path.join(path, default_name) if path else os.path.abspath(default_name)


def get_proxy(asset, proxy):
    asset_type = (
        "image"
        if asset.get("asset_type") == "document"
        else asset.get("asset_type", "stream")
    )
    if not proxy or proxy == "original":
        return ("original", asset["original"])

    if proxy not in PROXY_CASCADE:
        return asset.get(proxy)

    idx = PROXY_CASCADE.index(proxy)
    for level in PROXY_CASCADE[idx:]:
        for proxy in PROXY_TABLE.get((level, asset_type), []):
            if asset.get(proxy):
                return (proxy, asset[proxy])

    return ("original", asset["original"])


def download_stream(client, parent_id, root, proxy=None, capacity=10):
    os.makedirs(root, exist_ok=True)
    tracker = utils.PositionTracker(capacity)

    def download(name, file):
        proxy_name, url = get_proxy(file, proxy)
        asset_id = file["id"]
        name = filename(name, proxy_name)
        position = tracker.acquire()
        utils.download(url, name, position=position, desc=os.path.relpath(name, root))
        tracker.release(position)
        return {"destination": name, "source_id": asset_id}

    def make_folder(name, asset):
        os.makedirs(name, exist_ok=True)
        return {"destination": name, "source_id": asset["id"]}

    def make_asset(operation):
        name, asset = operation
        return (make_folder, download)[asset["_type"] == "file"](name, asset)

    for result in utils.parallelize(
        make_asset, remote_folder_stream(client, parent_id, root), capacity=capacity
    ):
        yield result


def remote_folder_stream(client, parent_id, root, recurse_vs=False):
    sibling_counter = Counter()
    for asset in stream_endpoint(f"/assets/{parent_id}/children"):
        name = os.path.join(root, asset["name"])
        sibling_counter[name] += 1
        if sibling_counter[name] > 1:
            base, ext = os.path.splitext(name)
            name = f"{base}_{sibling_counter[name]}{ext}"

        if asset["_type"] == "folder":
            yield (name, asset)

            for result in remote_folder_stream(client, asset["id"], name):
                yield result

        if recurse_vs and asset["_type"] == "version_stack":
            yield (name, asset)
            for result in remote_folder_stream(
                client, asset["id"], os.path.join(name, "versions")
            ):
                yield result

        elif asset["_type"] == "version_stack":
            yield (name, asset["cover_asset"])

        if asset["_type"] == "file":
            yield (name, asset)


def create_asset(client, parent_id, asset):
    if not parent_id or not asset:
        raise Exception(f'Both parent ID and asset must be specified - parent_id: {parent_id} | asset: {asset}')
    return client._api_call("post", f"/assets/{parent_id}/children", asset)


def upload_handler(
    client,
    queue: UploadQueue,
    queued_items: List,
    progress: Progress,
    progress_callback: Callable,
    capacity: int = 5,
):
    directories = {}
    tracker = utils.PositionTracker(capacity)

    def update_progress(task = None, value = None, start: bool = False):
        if start is True:
            return progress.start_task(task)
        else:
            return progress_callback(task, advance=value)

    def create_folder(parent_id, folder: Path, name: str):
        parent_id = directories.get(folder.parent)
        asset = create_asset(
            client,
            parent_id,
            { "type": "folder", "name": name },
        )
        directories[folder] = asset["id"]
        asset["source"] = folder
        return asset

    def upload_file(parent_id, filepath: Path, origin_path: Path):
        # Look up the destination remote ID based on a flat map of the dir paths we are keeping
        parent_id = directories.get(filepath.parent)
        name = filepath.name
        filepath_display = filepath.relative_to(origin_path)
        if filepath_display == '.':
            filepath_display = name
        filesize = filepath.stat().st_size
        filetype = mimetypes.guess_type(filepath)[0]
        this_task = progress.add_task(description=filepath_display, total=filesize, start=False)
        position = tracker.acquire()
        asset = create_asset(
            client,
            parent_id,
            {
                "name": name,
                "filesize": filesize,
                "filetype": filetype,
                "type": "file",
            },
        )
        uploader = FrameioUploader(
            asset,
            filepath,
            position,
            progress_callback = partial(
                update_progress,
                this_task,
            ),
        )
        result = uploader.upload()
        tracker.release(position)
        if result is True:
            asset['source'] = filepath.relative_to(origin_path)
            asset['outcome'] = 'Succeeded'
        else:
            logger.warning(f'{name}: upload did not complete successfully')
            asset['outcome'] = 'Failed'
        return asset
    
    def process_queued_item(item: QueuedAsset) -> dict:
        # Check to see if this file now has a remote side parent folder that we can place it in
        if directories.get(item.filepath.parent):
            destination_id = directories.get(item.filepath.parent)
        else:
            # Initially set the origin path -> ID
            directories[item.origin_path] = item.destination_id
            destination_id = item.destination_id
        if item.filepath.is_file():
            return upload_file(
                destination_id,
                item.filepath,
                origin_path = item.origin_path,
            )
        elif item.filepath.is_dir():
            # Create the asset folder
            folder = create_folder(
                parent_id = destination_id,
                folder = item.filepath,
                name = item.filepath.name,
            )
            if not folder:
                logger.error(f'Did not successfully create this folder asset on remote side: {item.filepath}')
                return
            # And store a relationship between that new asset ID and the dirpath
            directories[item] = folder['id']
            return folder
    
    for result in utils.exec_stream(
        process_queued_item,
        queued_items,
        capacity = capacity,
    ):
        yield result

