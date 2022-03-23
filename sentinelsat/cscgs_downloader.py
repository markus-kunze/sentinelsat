import json
from copy import copy
from datetime import date, datetime, timedelta
from urllib.parse import quote_plus, urljoin
import requests
import concurrent.futures
import itertools
import shutil
import threading
import time
from typing import Any, Dict
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

from sentinelsat.download import DownloadStatus, Downloader, _format_exception, _wait

from sentinelsat.exceptions import (
    InvalidChecksumError,
    LTAError,
    LTATriggered,
    SentinelAPIError,
    ServerError,
    UnauthorizedError,
    QueryLengthError,
    InvalidKeyError,
)


class LtaDownloader(Downloader):

    def __init__(self, api, *, node_filter=None, verify_checksum=True, fail_fast=False, n_concurrent_dl=None,
                 max_attempts=10, dl_retry_delay=10, lta_retry_delay=60, lta_timeout=None):
        super().__init__(api, node_filter=node_filter, verify_checksum=verify_checksum, fail_fast=fail_fast,
                         n_concurrent_dl=n_concurrent_dl, max_attempts=max_attempts, dl_retry_delay=dl_retry_delay,
                         lta_retry_delay=lta_retry_delay, lta_timeout=lta_timeout)

        from sentinelsat import LtaAPI
        self.api: LtaAPI = api
        self.verify_checksum = True

    def download_all(self, products: list = None, directory=".", batch_id: str = None):
        """Download a list of products.

        Parameters
        ----------
        products : list
            List of product IDs
        directory : string or Path, optional
            Directory where the files will be downloaded

        Notes
        ------
        By default, raises the most recent downloading exception if all downloads failed.
        If :attr:`Downloader.fail_fast` is set to True, raises the encountered exception on the first failed
        download instead.

        Returns
        -------
        dict[string, DownloadStatus]
            The status of all products.
        dict[string, Exception]
            Exception info for any failed products.
        dict[string, dict]
            A dictionary containing the product information for each product
            (unless the product was unavailable).
        """

        ResultTuple = namedtuple("ResultTuple", ["statuses", "exceptions", "product_infos"])

        assert self.n_concurrent_dl > 0
        if products is None and batch_id is not None:
            products = self.api.batch_order_ls_products(batch_id)
            if not isinstance(products, list):
                tmp_products = []
                tmp_products.append(products)
        elif products is None and batch_id is None:
            raise ValueError("Either products or batch_id must be provided")

        if len(products) == 0:
            return ResultTuple({}, {}, {})
        self.logger.info(
            "Will download %d products using %d workers", len(products), self.n_concurrent_dl
        )

        statuses, online_prods, offline_prods, product_infos, exceptions = self._init_statuses(products, batch_id=batch_id, directory=directory)

        # Skip already downloaded files.
        # Although the download method also checks, we do not need to retrieve such
        # products from the LTA and use up our quota.
        self._skip_existing_products(directory, offline_prods, product_infos, statuses, exceptions)

        stop_event = threading.Event()
        dl_tasks = {}
        trigger_tasks = {}

        # Two separate threadpools for downloading and triggering of retrieval.
        # Otherwise triggering might take up all threads and nothing is downloaded.
        dl_count = len(online_prods)
        dl_executor = ThreadPoolExecutor(
            max_workers=max(1, min(self.n_concurrent_dl, dl_count)),
            thread_name_prefix="dl",
        )
        dl_progress = self._tqdm(
            total=dl_count,
            desc="Downloading products",
            unit="product",
        )
        if offline_prods:
            trigger_executor = ThreadPoolExecutor(
                max_workers=min(self.api.concurrent_lta_trigger_limit, len(offline_prods)),
                thread_name_prefix="trigger",
            )
            trigger_progress = self._tqdm(
                total=len(offline_prods),
                desc="LTA retrieval",
                unit="product",
                leave=True,
            )
        try:
            # First all online products are downloaded. Subsequently, offline products that might
            # have become available in the meantime are requested.
            for pid in itertools.chain(online_prods):
                future = dl_executor.submit(
                    self._download_online_retry,
                    product_infos[pid],
                    directory,
                    statuses,
                    exceptions,
                    stop_event,
                )
                dl_tasks[future] = pid

            for pid in offline_prods:
                future = trigger_executor.submit(
                    self._trigger_and_wait,
                    pid,
                    stop_event,
                    statuses,
                )
                trigger_tasks[future] = pid

            for task in concurrent.futures.as_completed(list(trigger_tasks) + list(dl_tasks)):
                pid = trigger_tasks.get(task) or dl_tasks[task]
                exception = exceptions.get(pid)
                if task.cancelled():
                    exception = concurrent.futures.CancelledError()
                if task.exception():
                    exception = task.exception()

                if task in dl_tasks:
                    if not exception:
                        product_infos[pid] = task.result()
                        statuses[pid] = DownloadStatus.DOWNLOADED
                    dl_progress.update()
                    # Keep the LTA progress fresh
                    if offline_prods:
                        trigger_progress.update(0)
                else:
                    trigger_progress.update()
                    if all(t.done() for t in trigger_tasks):
                        trigger_progress.close()

                if exception:
                    exceptions[pid] = exception
                    if self.fail_fast:
                        raise exception from None
                    else:
                        self.logger.error("%s failed: %s", pid, _format_exception(exception))
        except:
            stop_event.set()
            for t in list(trigger_tasks) + list(dl_tasks):
                t.cancel()
            raise
        finally:
            dl_executor.shutdown()
            dl_progress.close()
            if offline_prods:
                trigger_executor.shutdown()
                trigger_progress.close()

        if not any(statuses):
            if not exceptions:
                raise SentinelAPIError("Downloading all products failed for an unknown reason")
            exception = list(exceptions)[0]
            raise exception

        # Update Online status in product_infos
        for pid, status in statuses.items():
            if status in [DownloadStatus.OFFLINE, DownloadStatus.TRIGGERED]:
                product_infos[pid]["Online"] = False
            elif status != DownloadStatus.UNAVAILABLE:
                product_infos[pid]["Online"] = True

        return ResultTuple(statuses, exceptions, product_infos)

    def _skip_existing_products(self, directory, products, product_infos, statuses, exceptions):
        for pid in list(products):
            product_info = product_infos[pid]
            try:
                filename = self.api._get_filename(product_info)
            except SentinelAPIError as e:
                exceptions[pid] = e
                if self.fail_fast:
                    raise
                self.logger.error(
                    "Getting filename for %s (%s) failed: %s",
                    product_info["Name"],
                    pid,
                    _format_exception(e),
                )
                continue
            path = Path(directory) / filename
            if path.exists():
                self.logger.info("Skipping already downloaded %s.", filename)
                product_info["path"] = str(path)
                statuses[pid] = DownloadStatus.DOWNLOADED
                products.remove(pid)
            else:
                self.logger.info(
                    "%s (%s) is in the LTA and retrieval will be triggered.",
                    product_info["Name"],
                    pid,
                )

    def _init_statuses(
        self, products: list, trigger_offline: bool = False, batch_id: str = None, directory="."
    ):
        statuses = {pid["Id"]: DownloadStatus.UNAVAILABLE for pid in products}
        online_prods = set()
        offline_prods = set()
        product_infos = {}
        exceptions = {}

        # Get online status and product info.
        for pid in self._tqdm(
            iterable=products, desc="Fetching archival status", unit="product", delay=2
        ):
            # self.logger.debug("_init_statuses: {}".format(pid))
            assert isinstance(pid, dict)
            product_infos[pid["Id"]] = pid
            product_infos[pid["Id"]]["downloaded_bytes"] = 0
            product_infos[pid["Id"]]["path"] = Path(directory) / product_infos[pid["Id"]]["Name"]
            product_infos[pid["Id"]]["size"] = product_infos[pid["Id"]]["ContentLength"]

            if batch_id is None:
                product_infos[pid["Id"]]["url"] = self.api._get_products_url(pid["Id"], "/$value")
                product_infos[pid["Id"]]["batch_product"] = False
                if product_infos[pid["Id"]]["Online"]:
                    statuses[pid["Id"]] = DownloadStatus.ONLINE
                    online_prods.add(pid["Id"])
                else:
                    statuses[pid["Id"]] = DownloadStatus.OFFLINE
                    offline_prods.add(pid["Id"])
            else:
                product_infos[pid["Id"]]["batch_product"] = True
                product_infos[pid["Id"]]["url"] = "{}odata/v1/BatchOrder({})/Products({})/$value".format(self.api.api_url, batch_id, pid["Id"])
                statuses[pid["Id"]] = DownloadStatus.ONLINE
                online_prods.add(pid["Id"])
        return statuses, online_prods, offline_prods, product_infos, exceptions

    def _trigger_and_wait(self, uuid, stop_event, statuses):
        """Continuously triggers retrieval of offline products

        This function is supposed to be called in a separate thread. By setting stop_event it can be stopped.
        """
        with self.api.lta_limit_semaphore:
            t0 = time.time()
            while True:
                if stop_event.is_set():
                    raise concurrent.futures.CancelledError()
                if self.lta_timeout and time.time() - t0 >= self.lta_timeout:
                    raise LTAError(
                        f"LTA retrieval for {uuid} timed out (lta_timeout={self.lta_timeout} seconds)"
                    )
                try:
                    if self.api.is_online(uuid):
                        break
                    if statuses[uuid] == DownloadStatus.OFFLINE:
                        # Trigger
                        triggered = self.trigger_offline_retrieval(uuid)
                        if triggered:
                            statuses[uuid] = DownloadStatus.TRIGGERED
                            self.logger.info(
                                "%s accepted for retrieval, waiting for it to come online...", uuid
                            )
                        else:
                            # Product is online
                            break
                except (LTAError, ServerError) as e:
                    self.logger.info(
                        "%s retrieval was not accepted: %s. Retrying in %d seconds",
                        uuid,
                        e.msg,
                        self.lta_retry_delay,
                    )
                _wait(stop_event, self.lta_retry_delay)
            self.logger.info("%s retrieval from LTA completed", uuid)
            statuses[uuid] = DownloadStatus.ONLINE

    def download(self, id, product_info=None, trigger_offline=True, wait_for_reload=True, directory=".", *, stop_event=None):
        if self.node_filter:
            return self._download_with_node_filter(id, directory, stop_event)

        # self.logger.debug("download, product_info: {}".format(product_info))

        if product_info is None:
            product_info = self.api.inspect_product(id)
            product_info["url"] = self.api._get_products_url(product_info["Id"], "/$value")

        filename = self.api._get_filename(product_info)
        path = Path(directory) / filename
        product_info["path"] = str(path)
        product_info["downloaded_bytes"] = 0
        product_info["size"] = product_info["ContentLength"]
        if "Checksum" in product_info:
            product_info[product_info["Checksum"][0]["Algorithm"].lower()] = product_info["Checksum"][0]["Value"]
        if "batch_product" not in product_info:
            product_info["batch_product"] = False

        if path.exists():
            # We assume that the product has been downloaded and is complete
            return product_info

        # An incomplete download triggers the retrieval from the LTA if the product is not online
        if product_info["batch_product"]:
            self._download_common(product_info, path, stop_event)
        elif product_info["Online"]:
            self._download_common(product_info, path, stop_event)
        elif not product_info["Online"]:
            if trigger_offline:
                self.logger.debug("Product {} is offline. Start triggering reload.".format(id))
                self.trigger_offline_retrieval(id)
            if wait_for_reload:
                self.logger.debug("Product {} is offline. Wait for it to be available.".format(id))
                if not self.wait_for_cscgs_offline_retrieval(id):
                    raise LTATriggered(id)
                self._download_common(product_info, path, stop_event)
            if not trigger_offline and not wait_for_reload:
                raise ValueError("Product is offline and needs to be reloaded. Please set trigger_offline and "
                                 "wait_for_reload to True to start offline retrieval.")

        return product_info

    def wait_for_cscgs_offline_retrieval(self, id):
        for cnt in range(self.max_attempts):
            if not self.api.is_online(id):
                time.sleep(self.lta_retry_delay)
            else:
                return True
        return False

    def _download_common(self, product_info: Dict[str, Any], path: Path, stop_event):
        # Use a temporary file for downloading
        temp_path = path.with_name(path.name + ".incomplete")
        skip_download = False
        if temp_path.exists():
            size = temp_path.stat().st_size
            if size > product_info["size"]:
                self.logger.warning(
                    "Existing incomplete file %s is larger than the expected final size"
                    " (%s vs %s bytes). Deleting it.",
                    str(temp_path),
                    size,
                    product_info["size"],
                )
                temp_path.unlink()
            elif size == product_info["size"]:
                if self.verify_checksum and not self.api._checksum_compare(temp_path, product_info):
                    # Log a warning since this should never happen
                    self.logger.warning(
                        "Existing incomplete file %s appears to be fully downloaded but "
                        "its checksum is incorrect. Deleting it.",
                        str(temp_path),
                    )
                    temp_path.unlink()
                else:
                    skip_download = True
            else:
                # continue downloading
                self.logger.info(
                    "Download will resume from existing incomplete file %s.", temp_path
                )
                pass
        if not skip_download:
            # Store the number of downloaded bytes for unit tests
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            product_info["downloaded_bytes"] = self._download_cscgs(
                product_info["url"],
                temp_path,
                product_info["ContentLength"],
                path.name,
                stop_event,
            )
        # Check integrity with MD5 checksum
        if self.verify_checksum is True:
            if not self.api._checksum_compare(temp_path, product_info):
                temp_path.unlink()
                raise InvalidChecksumError("File corrupt: checksums do not match")
        # Download successful, rename the temporary file to its proper name
        shutil.move(temp_path, path)
        return product_info

    def trigger_offline_retrieval(self, uuid):
        with self.api.dl_limit_semaphore:
            r = self.api.session.post(
                self.api._get_download_url(uuid), headers={"Range": "bytes=0-1"}
            )
        cause = r.headers.get("cause-message")
        # check https://scihub.copernicus.eu/userguide/LongTermArchive#HTTP_Status_codes
        if r.status_code in (200, 206):
            self.logger.debug("Product is online")
            return False
        elif r.status_code == 201:
            self.logger.debug("Accepted {} for retrieval".format(uuid))
            return True
        elif r.status_code == 202:
            self.logger.debug("Accepted {} for retrieval".format(uuid))
            return True
        elif r.status_code == 403 and cause and "concurrent flows" in cause:
            # cause: 'An exception occured while creating a stream: Maximum number of 4 concurrent flows achieved by the user "username""'
            self.logger.debug("Product is online but concurrent downloads limit was exceeded")
            return False
        elif r.status_code == 403:
            # cause: 'User 'username' offline products retrieval quota exceeded (20 fetches max) trying to fetch product PRODUCT_FILENAME (BYTES_COUNT bytes compressed)'
            msg = f"User quota exceeded: {cause}"
            self.logger.error(msg)
            raise LTAError(msg, r)
        elif r.status_code == 503:
            msg = f"Request not accepted: {cause}"
            self.logger.error(msg)
            raise LTAError(msg, r)
        elif r.status_code < 400:
            msg = f"Unexpected response {r.status_code}: {cause}"
            self.logger.error(msg)
            raise ServerError(msg, r)
        self.api._check_cscgs_response(r, test_json=False)

    def _download_online_retry(
        self, product_info, directory, statuses, exceptions, stop_event
    ):
        """Thin wrapper around download with retrying and checking whether a product is online

        Parameters
        ----------
        product_info : dict
        directory : string, optional
        statuses : dict of DownloadStatus
        exceptions : dict of Exception
        stop_event : threading.Event
        """
        if self.max_attempts <= 0:
            return

        uuid = product_info["Id"]
        title = product_info["Name"]

        # Wait for the triggering and retrieval to complete first
        while (
            statuses[uuid] != DownloadStatus.ONLINE
            and uuid not in exceptions
            and not stop_event.is_set()
        ):
            _wait(stop_event, 1)
        if uuid in exceptions:
            return

        last_exception = None
        for cnt in range(self.max_attempts):
            if stop_event.is_set():
                raise concurrent.futures.CancelledError()
            try:
                if cnt > 0:
                    _wait(stop_event, self.dl_retry_delay)
                statuses[uuid] = DownloadStatus.DOWNLOAD_STARTED
                # return self._download_common(
                #     product_info, path=product_info["path"], stop_event=stop_event
                # )
                return self.download(uuid, product_info, directory=directory, stop_event=stop_event)
            except (concurrent.futures.CancelledError, KeyboardInterrupt, SystemExit):
                raise
            except Exception as e:
                if isinstance(e, InvalidChecksumError):
                    self.logger.warning(
                        "Invalid checksum. The downloaded file for '%s' is corrupted.",
                        title,
                    )
                else:
                    self.logger.exception("There was an error downloading %s", title)
                retries_remaining = self.max_attempts - cnt - 1
                if retries_remaining > 0:
                    self.logger.info(
                        "%d retries left, retrying in %s seconds...",
                        retries_remaining,
                        self.dl_retry_delay,
                    )
                else:
                    self.logger.info("Downloading %s failed. No retries left.", title)
                last_exception = e
        raise last_exception

    def _download_cscgs(self, url, path, file_size, title, stop_event):
        headers = {}
        continuing = path.exists()
        if continuing:
            already_downloaded_bytes = path.stat().st_size
            headers = {"Range": "bytes={}-".format(already_downloaded_bytes)}
        else:
            already_downloaded_bytes = 0
        downloaded_bytes = 0
        with self.api.dl_limit_semaphore:
            r = self.api.session.get(url, stream=True, headers=headers)
        with self._tqdm(
            desc=f"Downloading {title}",
            total=file_size,
            unit="B",
            unit_scale=True,
            initial=already_downloaded_bytes,
        ) as progress, closing(r):
            self.api._check_cscgs_response(r, test_json=False)
            mode = "ab" if continuing else "wb"
            with open(path, mode) as f:
                iterator = r.iter_content(chunk_size=self.chunk_size)
                while True:
                    if stop_event and stop_event.is_set():
                        raise concurrent.futures.CancelledError()
                    try:
                        with self.api.dl_limit_semaphore:
                            chunk = next(iterator)
                    except StopIteration:
                        break
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        progress.update(len(chunk))
                        downloaded_bytes += len(chunk)
            # Return the number of bytes downloaded
            return downloaded_bytes
