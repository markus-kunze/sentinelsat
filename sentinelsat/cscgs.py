import json
from copy import copy
from urllib.parse import urljoin
import requests
import time
import logging
from collections import namedtuple
import requests.utils

from sentinelsat.sentinel import SentinelAPI, _parse_iso_date
from sentinelsat.models import *
from sentinelsat.download import DownloadStatus
from sentinelsat.cscgs_downloader import LtaDownloader

from sentinelsat.exceptions import (
    ServerError
)


class LtaAPI(SentinelAPI):
    logger = logging.getLogger("sentinelsat.LtaAPI")
    def __init__(
        self,
        user,
        password,
        api_url="https://apihub.copernicus.eu/apihub/",
        show_progressbars=True,
        timeout=60,
    ):
        # Try to use ~/.netrc if user or password is not provided
        if user is None or password is None:
            try:
                user, password = requests.utils.get_netrc_auth(api_url)
            except TypeError:
                pass

        super().__init__(
            user,
            password,
            api_url=api_url,
            show_progressbars=show_progressbars,
            timeout=timeout,
        )
        self.downloader = LtaDownloader(self)

    #
    # Download
    #

    def download(
        self,
        id,
        trigger_offline=True,
        wait_for_reload=True,
        directory_path=".",
        checksum=False,
        nodefilter=None,
    ):
        """Download a product.

        Uses the UUID on the server for the downloaded file, e.g.
        "173c2e04-6e06-30c1-a364-011e68bc4fad".

        Incomplete downloads are continued and complete files are skipped.

        Parameters
        ----------
        id : string
            UUID of the product, e.g. 'a8dd0cfd-613e-45ce-868c-d79177b916ed'
        directory_path : string, optional
            Where the file will be downloaded
        checksum : bool, default True
            If True, verify the downloaded file's integrity by checking its checksum.
            Throws InvalidChecksumError if the checksum does not match.
        nodefilter : callable, optional
            The callable is used to select which files of each product will be downloaded.
            If None (the default), the full products will be downloaded.
            See :mod:`sentinelsat.products` for sample node filters.

        Returns
        -------
        product_info : dict
            Dictionary containing the product's info from get_product_odata() as well as
            the path on disk.

        Raises
        ------
        InvalidChecksumError
            If the MD5 checksum does not match the checksum on the server.
        LTATriggered
            If the product has been archived and its retrieval was successfully triggered.
        LTAError
            If the product has been archived and its retrieval failed.
        """
        downloader = copy(self.downloader)
        downloader.node_filter = nodefilter
        downloader.verify_checksum = checksum
        return downloader.download(id, trigger_offline=trigger_offline, wait_for_reload=wait_for_reload,directory=directory_path)

    def download_all(
        self,
        products,
        directory_path=".",
        max_attempts: int = 10,
        checksum=False,
        n_concurrent_dl=None,
        lta_retry_delay=None,
        fail_fast=False,
        batch_id=None
    ):
        """Download a list of products.

        Takes a list of product IDs as input. This means that the return value of query() can be
        passed directly to this method.

        File names on the server are used for the downloaded files, e.g.
        "S1A_EW_GRDH_1SDH_20141003T003840_20141003T003920_002658_002F54_4DD1.zip".

        In case of interruptions or other exceptions, downloading will restart from where it left
        off. Downloading is attempted at most max_attempts times to avoid getting stuck with
        unrecoverable errors.

        Parameters
        ----------
        products : list
            List of product IDs
        directory_path : string
            Directory where the downloaded files will be downloaded
        max_attempts : int, default 10
            Number of allowed retries before giving up downloading a product.
        checksum : bool, default True
            If True, verify the downloaded files' integrity by checking its MD5 checksum.
            Throws InvalidChecksumError if the checksum does not match.
            Defaults to True.
        n_concurrent_dl : integer, optional
            Number of concurrent downloads. Defaults to :attr:`SentinelAPI.concurrent_dl_limit`.
        lta_retry_delay : float, default 60
            Number of seconds to wait between requests to the Long Term Archive.
        fail_fast : bool, default False
            if True, all other downloads are cancelled when one of the downloads fails.

        Notes
        -----
        By default, raises the most recent downloading exception if all downloads failed.
        If ``fail_fast`` is set to True, raises the encountered exception on the first failed
        download instead.

        Returns
        -------
        dict[string, dict]
            A dictionary containing the return value from download() for each successfully
            downloaded product.
        dict[string, dict]
            A dictionary containing the product information for products successfully
            triggered for retrieval from the long term archive but not downloaded.
        dict[string, dict]
            A dictionary containing the product information of products where either
            downloading or triggering failed. "exception" field with the exception info
            is included to the product info dict.
        """
        downloader = copy(self.downloader)
        downloader.verify_checksum = checksum
        downloader.fail_fast = fail_fast
        downloader.max_attempts = max_attempts
        if n_concurrent_dl:
            downloader.n_concurrent_dl = n_concurrent_dl
        if lta_retry_delay:
            downloader.lta_retry_delay = lta_retry_delay
        statuses, exceptions, product_infos = downloader.download_all(products, directory_path, batch_id=batch_id)

        # Adapt results to the old download_all() API
        downloaded_prods = {}
        retrieval_triggered = {}
        failed_prods = {}
        for pid, status in statuses.items():
            if pid not in product_infos:
                product_infos[pid] = {}
            if pid in exceptions:
                product_infos[pid]["exception"] = exceptions[pid]
            if status == DownloadStatus.DOWNLOADED:
                downloaded_prods[pid] = product_infos[pid]
            elif status == DownloadStatus.TRIGGERED:
                retrieval_triggered[pid] = product_infos[pid]
            else:
                failed_prods[pid] = product_infos[pid]
        ResultTuple = namedtuple("ResultTuple", ["downloaded", "retrieval_triggered", "failed"])
        return ResultTuple(downloaded_prods, retrieval_triggered, failed_prods)

    #
    # Query
    #

    @staticmethod
    def generate_query(
        name_query: NameQuery = NameQuery(query=""),
        publication_date_query: PublicationDateQuery = PublicationDateQuery([]),
        sensing_date_query: SensingDateQuery = SensingDateQuery(""),
        area_query: AreaQuery = AreaQuery(""),
    ):

        query = ""
        for count, query_part in enumerate(
            [
                name_query.query,
                publication_date_query.query,
                sensing_date_query.query,
                area_query.query,
            ]
        ):
            if query_part is not None or query_part != "" and count == 0:
                query += query_part
            elif query_part is not None and query_part != "":
                query += " and {}".format(query_part)
        return query

    def query(self, query, additional_options=None, skip_odata_output: bool = True):
        if additional_options is None:
            additional_options = []

        with self.dl_limit_semaphore:
            response = self.session.get(
                self._format_url(additional_options), params={"$filter": query}
            )
        self._check_cscgs_response(response, query_string=query)

        if skip_odata_output:
            return response.json()["value"]
        else:
            return response.json()

    #
    # Single product methods
    #

    def inspect_product(self, id):
        url = self._get_products_url(id)
        with self.dl_limit_semaphore:
            response = self.session.get(url)
        self._check_cscgs_response(response)
        # values = _parse_cscda_response(response.json())
        return response.json()

    def is_online(self, id):
        """Returns whether a product is online

        Parameters
        ----------
        id : string
            UUID of the product, e.g. 'a8dd0cfd-613e-45ce-868c-d79177b916ed'

        Returns
        -------
        bool
            True if online, False if in LTA

        See Also
        --------
        :meth:`SentinelAPI.trigger_offline_retrieval()`
        """

        if not self._online_attribute_used:
            return True
        url = self._get_products_url(id, "?$format=json")
        with self.dl_limit_semaphore:
            r = self.session.get(url)
        try:
            self._check_scihub_response(r)
        except ServerError as e:
            # Handle DHuS versions that do not set the Online attribute
            if "Could not find property with name: 'Online'" in e.msg:
                self._online_attribute_used = False
                return True
            raise
        return r.json()["Online"]

    #
    # Bulk methods
    #

    def bulk_create(self, query: str, batch_size_products: int = 30, batch_size_volume: int = 50):
        filter_param = "{}".format(query)
        filter_param = filter_param.replace("'", "")

        bulk_create_body = {
            "@odata.context": "$metadata#Bulk/$entity",
            "FilterParam": filter_param,
            "BatchsizeProducts": batch_size_products,
            "BatchsizeVolume": batch_size_volume,
        }
        bulk_create_uri = "{}odata/v1/Bulks".format(self.api_url)
        self.logger.debug("bulk_create_body: {}".format(bulk_create_body))

        with self.dl_limit_semaphore:
            response = self.session.post(bulk_create_uri, data=json.dumps(bulk_create_body))

        self._check_cscgs_response(response, query_string=query)

        return response.json()

    def bulk_ls(self, skip_odata_output: bool = True):
        query_batches_uri = "{}odata/v1/Bulks".format(self.api_url)
        with self.dl_limit_semaphore:
            response = self.session.get(query_batches_uri)
        self._check_cscgs_response(response)
        if skip_odata_output:
            return response.json()["Bulks"]
        else:
            return response.json()

    def bulk_inspect(self, bulk_id: str):
        query_batches_uri = "{}odata/v1/Bulk({})".format(self.api_url, bulk_id)
        with self.dl_limit_semaphore:
            response = self.session.get(query_batches_uri)
        self._check_cscgs_response(response)
        return response.json()

    def bulk_delete(self, bulk_id: str, delete_batches: bool = False):
        if delete_batches:
            batches = self.bulk_ls_batches(bulk_id)
            for batch in batches:
                self.batch_order_delete(batch["Id"], ignore_errors=True)

        delete_bulk_uri = "{}odata/v1/Bulk({})".format(self.api_url, bulk_id)
        with self.dl_limit_semaphore:
            response = self.session.delete(delete_bulk_uri)
        self._check_cscgs_response(response, test_json=False)
        return True

    def bulk_auto_download(
        self,
        query,
        batch_size_products: int = 30,
        batch_size_volume: int = 50,
        directory=".",
        *,
        stop_event=None,
        checksum=False
    ):
        bulk_create_resp = self.bulk_create(
            query, batch_size_products=batch_size_products, batch_size_volume=batch_size_volume
        )
        bulk_id = bulk_create_resp["Id"]
        self.logger.info("Created Bulk with id: {}".format(bulk_id))
        batches = self.bulk_ls_batches(bulk_id)
        self.logger.info(
            "batches contained in the bulk with id {}: \n{}".format(
                bulk_id, json.dumps(batches, indent=2)
            )
        )
        self.batches_auto_download(batches)
        self.bulk_delete(bulk_id=bulk_id, delete_batches=True)

    def bulk_ls_batches(self, bulk_id: str):
        query_batches_uri = "{}odata/v1/Bulk({})/BatchOrders".format(self.api_url, bulk_id)
        with self.dl_limit_semaphore:
            response = self.session.get(query_batches_uri)
        self._check_scihub_response(response)
        return response.json()["value"]

    #
    # Batch methods
    #

    def batch_order_delete(self, batch_id: str, ignore_errors=False):
        delete_batches_uri = "{}odata/v1/BatchOrder({})".format(self.api_url, batch_id)
        with self.dl_limit_semaphore:
            response = self.session.delete(delete_batches_uri)
        if not ignore_errors:
            self._check_cscgs_response(response, test_json=False)
        return True

    def batches_auto_download(self, batches: list):
        for batch in batches:
            batch_id = batch["Id"]
            if not self.batch_order_completed(batch_id):
                self.batch_order_trigger(batch_id)
                self.batch_order_wait_completed(batch_id)
            self.downloader.download_all(batch_id=batch_id)

    def batch_order_products_download(
        self,
        batch_id: str,
        directory=".",
        *,
        checksum=False,
        nodefilter=None
    ):
        downloader = copy(self.downloader)
        downloader.node_filter = nodefilter
        downloader.verify_checksum = checksum
        return downloader.download_all(batch_id=batch_id, directory=directory)

    def batch_order_inspect(self, batch_id: str):
        batch_inspect_url = "{}odata/v1/BatchOrder({})".format(self.api_url, batch_id)
        with self.dl_limit_semaphore:
            response = self.session.get(batch_inspect_url)
        self._check_cscgs_response(response)
        return response.json()

    def batch_order_completed(self, batch_id: str):
        batch_inspect_url = "{}odata/v1/BatchOrder({})".format(self.api_url, batch_id)
        with self.dl_limit_semaphore:
            response = self.session.get(batch_inspect_url)
        self._check_cscgs_response(response)
        return response.json()["value"][0]["Status"] == "completed"

    def batch_order_wait_completed(
        self, batch_id: str, interval: int = 20000, max_wait: int = 3600000
    ):
        cnt_waited = 0
        while cnt_waited <= max_wait:
            if not self.batch_order_completed(batch_id):
                time.sleep(interval / 1000)
                cnt_waited += interval
                self.logger.info("Waited {}ms for batch order {}".format(cnt_waited, batch_id))
            else:
                return True
        return False

    def batch_order_ls_products(self, batch_id: str) -> list:
        batch_products_uri = "{}odata/v1/BatchOrder({})/Products".format(self.api_url, batch_id)
        with self.dl_limit_semaphore:
            response = self.session.get(batch_products_uri)
        self._check_cscgs_response(response)
        return response.json()["value"]

    def batch_order_ls_product_ids(self, batch_id: str):
        batch_products_uri = "{}odata/v1/BatchOrder({})/Products".format(self.api_url, batch_id)
        with self.dl_limit_semaphore:
            response = self.session.get(batch_products_uri)
        self._check_cscgs_response(response)

        ids = []
        for product in response.json()["value"]:
            ids.append(product["Id"])

        return ids

    def batch_order_trigger(self, batch_id: str, priority=10):
        batch_trigger_uri = "{}odata/v1/BatchOrder({})/OData.CSC.BatchOrder".format(
            self.api_url, batch_id
        )

        with self.dl_limit_semaphore:
            response = self.session.post(batch_trigger_uri, data=json.dumps({"Priority": priority}))
        self._check_cscgs_response(response, expected_status_code=204, test_json=False)
        self.logger.info("Successful triggered batch with id {}".format(batch_id))

    @staticmethod
    def _check_cscgs_response(
            response, expected_status_code=None, test_json=True, query_string=None
    ):
        response.encoding = "utf-8"
        if response.status_code == expected_status_code:
            if test_json:
                response.json()
            pass

        response.raise_for_status()
        if test_json:
            response.json()

    #
    # Helpers
    #

    def _format_url(self, additional_options=None):
        url = "/odata/v1/Products?"
        for option in additional_options:
            url += option
        return urljoin(self.api_url, url)

    def _get_filename(self, product_info):
        return product_info["Name"]

    def _get_products_url(self, uuid, suffix=""):
        return "{}odata/v1/Products({}){}".format(self.api_url, uuid, suffix)

    def _get_query_url(self, suffix=""):
        return self.api_url + f"odata/v1/Products?" + suffix

    def _get_download_url(self, uuid):
        return self._get_products_url(uuid, "/OData.CSC.Order")


def _parse_cscda_response(product):
    output = {
        "id": product["Id"],
        "title": product["Name"],
        "size": int(product["ContentLength"]),
        product["Checksum"][0]["Algorithm"].lower(): product["Checksum"][0]["Value"],
        "date": _parse_iso_date(product["ContentDate"]["Start"]),
        "footprint": product["Footprint"],
        "url": None,
        "Online": product.get("Online", True),
        "Origin Date": _parse_iso_date(product["OriginDate"]),
        "Publication Date": _parse_iso_date(product["PublicationDate"]),
    }
    # Parse the extended metadata, if provided
    converters = [int, float, _parse_iso_date]
    if "Attributes" in product:
        for attr in product["Attributes"].get("results", []):
            value = attr["Value"]
            for f in converters:
                try:
                    value = f(attr["Value"])
                    break
                except ValueError:
                    pass
            output[attr["Name"]] = value
    return output

