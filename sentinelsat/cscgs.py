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

from sentinelsat.exceptions import ServerError


class LtaAPI(SentinelAPI):
    """Class to connect to Copernicus Space Comptonent Ground Segment
    Long Term Archive, search and download products.

    Parameters
    ----------
    user : string
        username for CSCGS LTA
        set to None to use ~/.netrc
    password : string
        password for CSCGS LTA
        set to None to use ~/.netrc
    api_url : string, optional
        URL of the CSCGS LTA
        defaults to 'https://acs.clas-aip.de'
    show_progressbars : bool
        Whether progressbars should be shown or not, e.g. during download. Defaults to True.
    timeout : float or tuple, default 60
        How long to wait for DataHub response (in seconds).
        Tuple (connect, read) allowed.
        Set to None to wait indefinitely.
    """

    logger = logging.getLogger("sentinelsat.LtaAPI")

    def __init__(
        self,
        user,
        password,
        api_url="https://acs.clas-aip.de",
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

        Incomplete downloads are continued and completed files are skipped.

        Parameters
        ----------
        id : string
            UUID of the product, e.g. '173c2e04-6e06-30c1-a364-011e68bc4fad'
        trigger_offline: bool, optional
            starts offline retrieval
        wait_for_reload: bool, optional
            Wait until the product is online and then start the download.
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
            Dictionary containing the product's info from inspect_download() as well as
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
        return downloader.download(
            id,
            trigger_offline=trigger_offline,
            wait_for_reload=wait_for_reload,
            directory=directory_path,
        )

    def download_all(
        self,
        products: list = None,
        batch_id: str = None,
        directory_path=".",
        max_attempts: int = 10,
        checksum=False,
        n_concurrent_dl=None,
        lta_retry_delay=None,
        fail_fast=False,
    ):
        """Download a list of products or a batch.

        Takes a list of product IDs or a batch ID as input. This means that the return value of query() can be
        passed directly to this method.

        Metadata names on the server are used for the downloaded files, e.g.
        "S1A_EW_GRDH_1SDH_20141003T003840_20141003T003920_002658_002F54_4DD1.zip".

        In case of interruptions or other exceptions, downloading will restart from where it left
        off. Downloading is attempted at most max_attempts times to avoid getting stuck with
        unrecoverable errors.

        Parameters
        ----------
        products : list, optional
            List of product IDs
        batch_id: bool, optional
            The ID of the batch to be downloaded.
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
        statuses, exceptions, product_infos = downloader.download_all(
            products, directory_path, batch_id=batch_id
        )

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
        """Generate a query by using sentinelsat.models.

        To avoid having to deal with the OData syntax, this method offers an easy way to generate a query with
        the available classes. Several query types can be used.  This query can be used directly in the query()
        method.

        Parameters
        ----------
        name_query: NameQuery, Optional
            A query based on the name of a product e.g. "startswith(Name,'S1B_EW_')"
        publication_date_query: PublicationDateQuery, optional
            A query based on publication dates of products. This may include several time intervals e.g.
            "PublicationDate gt 2017-05-15T00:00:00.000Z and PublicationDate lt 2017-05-16T00:00:00.000Z"
        sensing_date_query: SensingDateQuery, optional
            A query based on sensing dates of products. This may include several time intervals e.g.
            "ContentDate/Start gt 2019-05-15T00:00:00.000Z and ContentDate/End lt 2019-05-16T00:00:00.000Z"
        area_query: AreaQuery, optional
            A query based on an wkt formated citeria e.g.
            "POLYGON((-127.89734578345 45.234534534,-127.89734578345 45.234534534,-127.8973457834545.234534534,
            -127.89734578345 45.234534534))"

        Returns
        -------
        string
            The formatted and ready to use query.
        """

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
        """Execute a query to the CSCGS LTA API with query string.

        Parameters
        ----------
        query: sting
            The formatted query string
        additional_options: string
            Not yet supported
        skip_odata_output: bool, optional
            The CSCGS LTA API returns OData formatted JSON, this option is a way to get only
            the relevant information.

        Returns
        -------
        list[dict[string, any]]
            Products returned by the query as a list.
        """

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
            return [response.json()]

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

    def bulk_create(
        self, query: str, batch_size_products: int = 30, batch_size_volume: int = 1000000000
    ):
        """Create a Bulk from the results of a query. You can configure the size of the resulting Batches.

        Parameters
        ----------
        query: str
            The filter criterion that specifies the products that the Bulk should contain.
        batch_size_products: int
            The maximum number of products making up each child Batch.
        batch_size_volume: int
            The maximum volume of each child Batch making up the Bulk in byte.

        Returns
        -------
        dict[str, any]:
            The Bulk information returned by the AIP e.g.
            {
              "Id": "string",
              "Status": "string",
              "StatusMessage": "string",
              "FilterParam": "string",
              "OrderParam": "string",
              "BatchsizeProducts": 0,
              "BatchsizeVolume": 0,
              "SubmissionDate": "string",
              "CompletedDate": "string",
              "NotificationEndpoint": "string",
              "NotificationEpUsername": "string",
              "NotificationEpPassword": "string"
            }
        """
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
        """List all your Bulks (user specific).

        Parameters
        ----------
        skip_odata_output: bool, optional
            The CSCGS LTA API returns OData formatted JSON, this option is a way to get only
            the relevant information.

        Returns
        -------
        list[dict[str, any]]:
            List of your bulks e.g.:
            [
                {
                  "Id": "string",
                  "Status": "string",
                  "StatusMessage": "string",
                  "FilterParam": "string",
                  "OrderParam": "string",
                  "BatchsizeProducts": 0,
                  "BatchsizeVolume": 0,
                  "SubmissionDate": "string",
                  "CompletedDate": "string",
                  "NotificationEndpoint": "string",
                  "NotificationEpUsername": "string",
                  "NotificationEpPassword": "string"
                }
          ]
        """

        query_batches_uri = "{}odata/v1/Bulks".format(self.api_url)
        with self.dl_limit_semaphore:
            response = self.session.get(query_batches_uri)
        self._check_cscgs_response(response)
        if skip_odata_output:
            return response.json()["Bulks"]
        else:
            return [response.json()]

    def bulk_inspect(self, bulk_id: str):
        """Access the API to get information about a specific bulk.

        Returns a dict containing the Id, Status and many more attributes of the Bulk.

        Parameters
        ----------
        bulk_id: str
            The UUID of the Bulk to query

        Returns
        -------
        dict[str, any]
            A dictionary with an item for each attribute of the Bulk e.g.:
            {
              "Id": "string",
              "Status": "string",
              "StatusMessage": "string",
              "FilterParam": "string",
              "OrderParam": "string",
              "BatchsizeProducts": 0,
              "BatchsizeVolume": 0,
              "SubmissionDate": "string",
              "CompletedDate": "string",
              "NotificationEndpoint": "string",
              "NotificationEpUsername": "string",
              "NotificationEpPassword": "string"
            }
        """
        query_batches_uri = "{}odata/v1/Bulk({})".format(self.api_url, bulk_id)
        with self.dl_limit_semaphore:
            response = self.session.get(query_batches_uri)
        self._check_cscgs_response(response)
        return response.json()

    def bulk_delete(self, bulk_id: str, delete_batches: bool = False):
        """Commission the AIP to delete a specific Bulk.

        All Batches of Bulk have to be deleted before a Bulk can be deleted. So this
        method provides the option to also delete the Batches of the Bulk before the
        Bulk deletion.

        Parameters
        ----------
        bulk_id: str
            The UUID of the Bulk to be deleted
        delete_batches: bool, optional
            Specifies whether the Batches of Bulk shall be deleted before the Bulk gets
            deleted.

        Returns
        -------
        bool:
            Indicates whether the Bulk could be deleted or not
        """
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
        batch_size_volume: int = 1000000000,
        directory=".",
        *,
        checksum=False
    ):
        """Automates the handling of a Bulk to download data.

        This involves:
        * Bulk create
        * Get Batches of the Bulk
        * Trigger Batches
        * Wait for Batch completion
        * Download Batch
        * Delete Batch
        * Delete Bulk

        Parameters
        ----------
        query: str
            The filter criterion that specifies the products that the Bulk should contain.
        batch_size_products: int
            The maximum number of products making up each child Batch.
        batch_size_volume: int
            The maximum volume of each child Batch making up the Bulk in byte.
        directory : string, optional
            Where the file will be downloaded

        Returns
        -------
        None

        See Also
        --------
        :meth:`LtaAPI.batches_auto_download()`
        """
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
        self.batches_auto_download(batches=batches, directory=directory, checksum=checksum)
        self.bulk_delete(bulk_id=bulk_id, delete_batches=True)

    def bulk_ls_batches(self, bulk_id: str, skip_odata_output: bool = True):
        """Access the API to get the Batches of a Bulk.

        Returns a List containing the Batches of a Bulk.

        Parameters
        ----------
        bulk_id: str
            The UUID of the Bulk to query
        skip_odata_output: bool, optional
            The CSCGS LTA API returns OData formatted JSON, this option is a way to get only
            the relevant information.

        Returns
        -------
        list[dict[str, any]]
            A list with a dict for each Batch e.g.:
            [
                {
                  "Id": "string",
                  "Status": "string",
                  "StatusMessage": "string",
                  "OrderSize": 0,
                  "SubmissionDate": "string",
                  "EstimatedDate": "string",
                  "CompletedDate": "string",
                  "Priority": 0
                }
            ]
        """
        query_batches_uri = "{}odata/v1/Bulk({})/BatchOrders".format(self.api_url, bulk_id)
        with self.dl_limit_semaphore:
            response = self.session.get(query_batches_uri)
        self._check_scihub_response(response)
        if skip_odata_output:
            return response.json()["value"]
        else:
            return [response.json()]

    #
    # Batch methods
    #

    def batch_order_delete(self, batch_id: str, ignore_errors=False):
        """Commission the AIP to delete a specific Batch of a Bulk.

        Parameters
        ----------
        batch_id: str
            The UUID of the Batch to be deleted
        ignore_errors: bool, optional
            Specifies whether errors be ignored

        Returns
        -------
        bool:
            Indicates whether the Batch could be deleted or not
        """

        delete_batches_uri = "{}odata/v1/BatchOrder({})".format(self.api_url, batch_id)
        with self.dl_limit_semaphore:
            response = self.session.delete(delete_batches_uri)
        if not ignore_errors:
            self._check_cscgs_response(response, test_json=False)
        return True

    def batches_auto_download(self, batches: list, directory=".", checksum=False):
        """Automates the handling of a Batch to download data.

        This involves:
        * Trigger Batches
        * Wait for Batch completion
        * Download Batch

        Parameters
        ----------
        batches: list
            The filter criterion that specifies the products that the Bulk should contain.
        directory : string, optional
            Where the file will be downloaded
        checksum: bool
            If True, verify the downloaded files' integrity by checking its MD5 checksum.
            Throws InvalidChecksumError if the checksum does not match.

        Returns
        -------
        None

        See Also
        --------
        :meth:`LtaAPI.batch_order_completed()`
        :meth:`LtaAPI.batch_order_trigger()`
        :meth:`LtaAPI.batch_order_wait_completed()`
        """
        downloader = copy(self.downloader)
        downloader.verify_checksum = checksum

        for batch in batches:
            batch_id = batch["Id"]
            if not self.batch_order_completed(batch_id):
                self.batch_order_trigger(batch_id)
                self.batch_order_wait_completed(batch_id)
            downloader.download_all(batch_id=batch_id, directory=directory)

    def batch_order_products_download(self, batch_id: str, directory=".", *, checksum=False):
        """Downloads the products of a Batch.

        The Batch must have the status completed.

        Parameters
        ----------
        batch_id: list
            The ID of the batch to be downloaded
        directory : string, optional
            Where the file will be downloaded
        checksum : bool, default False
            If True, verify the downloaded files' integrity by checking its MD5 checksum.
            Throws InvalidChecksumError if the checksum does not match.

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
        return downloader.download_all(batch_id=batch_id, directory=directory)

    def batch_order_inspect(self, batch_id: str):
        """Access the API to get information about a specific Batch Order.

        Returns a dict containing the Id, Status and many more attributes of the Batch.

        Parameters
        ----------
        batch_id: str
            The UUID of the Batch to query

        Returns
        -------
        dict[str, any]
            A dictionary with an item for each attribute of the Batch e.g.:
            {
              "Id": "string",
              "Status": "string",
              "StatusMessage": "string",
              "OrderSize": 0,
              "SubmissionDate": "string",
              "EstimatedDate": "string",
              "CompletedDate": "string",
              "Priority": 0
            }
        """
        batch_inspect_url = "{}odata/v1/BatchOrder({})".format(self.api_url, batch_id)
        with self.dl_limit_semaphore:
            response = self.session.get(batch_inspect_url)
        self._check_cscgs_response(response)
        return response.json()

    def batch_order_completed(self, batch_id: str):
        """Returns whether a Batch is completed

        Parameters
        ----------
        batch_id : string
            UUID of the Batch, e.g. 'a8dd0cfd-613e-45ce-868c-d79177b916ed'

        Returns
        -------
        bool
            True if completed, False not

        See Also
        --------
        :meth:`LtaAPI.batch_order_trigger()`
        """
        batch_inspect_url = "{}odata/v1/BatchOrder({})".format(self.api_url, batch_id)
        with self.dl_limit_semaphore:
            response = self.session.get(batch_inspect_url)
        self._check_cscgs_response(response)
        return response.json()["value"][0]["Status"] == "completed"

    def batch_order_wait_completed(
        self, batch_id: str, timeout: int = 20000, max_wait: int = 3600000
    ):
        """Waits for a Batch to be in status completed

        This returns True if status turn completed and if not it return False

        Parameters
        ----------
        batch_id: string
            UUID of the Batch, e.g. 'a8dd0cfd-613e-45ce-868c-d79177b916ed'
        timeout: int, optional
            Time in seconds to wait for the next try.
        max_wait: int, optional
            Maximum time to wait in seconds.

        Returns
        -------
        bool
            True if completed, False not

        See Also
        --------
        :meth:`LtaAPI.batch_order_trigger()`
        """
        cnt_waited = 0
        while cnt_waited <= max_wait:
            if not self.batch_order_completed(batch_id):
                time.sleep(timeout / 1000)
                cnt_waited += timeout
                self.logger.info("Waited {}ms for batch order {}".format(cnt_waited, batch_id))
            else:
                return True
        return False

    def batch_order_ls_products(self, batch_id: str) -> list:
        """List the products of a Batch

        Returns a list containing dict containing the metadata of a product.

        Parameters
        ----------
        batch_id: str
            UUID of the Batch, e.g. 'a8dd0cfd-613e-45ce-868c-d79177b916ed'

        Returns
        -------
        list[dict[str, any]]
            List containing dict containing the metadata of a product e.g.:
            [
                {
                  "Id": "string",
                  "Name": "string",
                  "ContentType": "string",
                  "ContentLength": 0,
                  "OriginDate": "string",
                  "PublicationDate": "string",
                  "ModificationDate": "string",
                  "Online": true,
                  "EvictionDate": "string",
                  "Checksums": [
                    {
                      "Algorithm": "string",
                      "Value": "string",
                      "Date": "string"
                    }
                  ],
                  "ContentDate": {
                    "Start": "string",
                    "End": "string"
                  },
                  "Footprint": "string",
                  "Attributes": [
                    {
                      "Name": "string",
                      "ValueType": "string",
                      "Value": "string"
                    }
                  ]
                }
            ]
        See Also
        --------
        :meth:`LtaAPI.batch_order_ls_product_ids()`
        """
        batch_products_uri = "{}odata/v1/BatchOrder({})/Products".format(self.api_url, batch_id)
        with self.dl_limit_semaphore:
            response = self.session.get(batch_products_uri)
        self._check_cscgs_response(response)
        return response.json()["value"]

    def batch_order_ls_product_ids(self, batch_id: str):
        """List the products of a Batch

        Returns a list containing dict containing the metadata of a product.

        Parameters
        ----------
        batch_id: str
            UUID of the Batch, e.g. 'a8dd0cfd-613e-45ce-868c-d79177b916ed'

        Returns
        -------
        list[dict[str, any]]
            List containing dict containing the metadata of a product e.g.:
            [
                "cb1fb0ee-7566-4ddb-b765-26fd2d4a636d",
                "3153451a-53c1-46d0-9c68-cebc0190bab9"
            ]
        See Also
        --------
        :meth:`LtaAPI.batch_order_ls_products()`
        """
        batch_products_uri = "{}odata/v1/BatchOrder({})/Products".format(self.api_url, batch_id)
        with self.dl_limit_semaphore:
            response = self.session.get(batch_products_uri)
        self._check_cscgs_response(response)

        ids = []
        for product in response.json()["value"]:
            ids.append(product["Id"])

        return ids

    def batch_order_trigger(self, batch_id: str, priority=10):
        """Triggers retrieval of a whole Batch from the Long Term Archive.

        Parameters
        ----------
        batch_id : string
            UUID of the Batch
        priority: int
            Priority of the Batch. It is an integer from 1-100, where 100 is the highest priority.

        Returns
        -------
        None

        Raises
        ------
        LTAError
            If the request was not accepted due to exceeded user quota or server overload.
        ServerError
            If an unexpected response was received from server.
        UnauthorizedError
            If the provided credentials were invalid.
        """
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
