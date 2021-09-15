from pathlib import Path
import math
import matplotlib.pyplot as plt
import pandas as pd
from sentinelsat import SentinelAPI
from datetime import date
import logging
from concurrent.futures import ThreadPoolExecutor
import time
from .ssat_database import SSatDatabase


class ApiUrl:
    copernicus_hub = 'https://scihub.copernicus.eu/dhus'


class SSatDownloader:
    base_columns = ['tile', 'date', 'platformserialidentifier', 'processinglevel', 'producttype', 'online',
                    'downloaded', 'qlook']

    def __init__(self, user, password, api=ApiUrl.copernicus_hub, logger_level=logging.INFO):

        # create a logger

        logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',
                            level=logging.WARNING, datefmt='%I:%M:%S')

        self.logger = logging.getLogger('sentineldownloader.SSatDownloader')
        self.logger.setLevel(logger_level)

        # create a connection to the api
        self.api = SSatDownloader.create_connection(user, password, api, self.logger)

        # stores output directory as Path and check if it exists

        # create the base databases
        self.database = SSatDatabase(self, logger_level)
        self._search_df = None

        # Create the basic variables
        self.query_args = self.status = None

        # Variables that control the background download
        self.executor = self.monitor = self.result = None

    def open_database(self, folder):
        """
        Open the database on the specified folder
        :return: None
        """
        self.database.open(folder)

    def create_database(self, folder):
        """
        Open the database on the specified folder
        :return: None
        """
        self.database.create(folder)

    def clear(self):
        """Clear the instance variables"""
        self._search_df = None
        self.query_args = self.status = None
        self.executor = self.monitor = self.result = None

    def connect(self, user: str, password: str, api: str):
        """Makes a new connection and tests it"""
        self.api = SSatDownloader.create_connection(user, password, api)

    def query(self, post_filter=None, search_online=False, **kwargs):
        """
        Performs a fundamental query to the API.
        :param post_filter: Filter to be applied after the initial query.
        :param search_online: If True, check if the images are available online or need to be requested to LTA
        :param kwargs: arguments to be passed to sentinelsat's query.
        Ex. (filename='*23KKQ*', date=('20190101', '20200101'), producttyp='S2MSI2A', ...)
        For all the options, check our Copernicus Open Access Hub documentation.
        https://sentinelsat.readthedocs.io/en/stable/api_overview.html#:~:text=Copernicus%20Open%20Access%20Hub%20documentation.
        :return:None
        """
        # check if there is a valid connection
        if not self._check_api or self._is_downloading:
            return

        self.query_args = kwargs

        products = self.api.query(**kwargs)
        self._search_df = self.api.to_dataframe(products).sort_values(by='beginposition')

        # apply post filter before proceeding
        self.filter_results(query=post_filter)

        # Reformat description
        self._search_df['date'] = self._search_df['title'].str[11:19]
        self._search_df['tile'] = self._search_df['title'].str[39:44]

        # Check if product is online
        if search_online:
            print(f'Retrieving product online status. This may take a while...')
            self._search_df['online'] = self._search_df.index.map(self.api.is_online)
        else:
            self._search_df['online'] = False

        # update local information
        self.update_local_info()

        print(self.summary)

    def update(self, local=True):
        """
        Update the products dataframe. If local is True, update only local info and don't make a new query to the api.
        """
        # check the dataset
        if not self.check_search_df(display_msg=True):
            return

        if local:
            self.update_local_info()
        else:
            if self._check_api and not self._is_downloading:
                self.query(**self.query_args)

    def update_local_info(self):
        # update the database
        self.database.update()

        # check the search df
        if not self.check_search_df(display_msg=True):
            return

        # check the database
        if not self.database.initialized(display_message=True):
            self.logger.warning("Downloaded and qlook status will be set to False")
            self.logger.warning("To correct this information, open or create a database and run .update_local_info()")

            self._search_df['downloaded'] = False
            self._search_df['qlook'] = False

        else:
            # Check if products are already downloaded in the output directory
            self._search_df['downloaded'] = self.images_targets.map(Path.exists)

            # Check if products are already in the output directory
            self._search_df['qlook'] = self.quick_looks_targets.map(Path.exists)

    def filter_results(self, query):
        """
        Filters the resulting DataFrame, by applying a query to it. Any column can be used in the query:
        ex. filter_results(query='cloudcoverpercentage < 80')
        """
        if not self.check_search_df(display_msg=True):
            return

        self._search_df = self._search_df.query(query) if query is not None else self._search_df

    def get_quick_looks(self):
        """
        Get the quick looks and put them in the qlooks_dir subfolder.
        """
        if not self.check_search_df(display_msg=True) or not self.database.initialized(display_message=True):
            return

        subset = self.combined_df[~self.combined_df['qlook']]

        self.api.download_all_quicklooks(subset.index, directory_path=str(self.database.qlooks_folder))

        self.update_local_info()

    def _monitor_download(self):
        """Monitor for the downloading thread to complete, every 60 secs"""
        while self.result.running():
            time.sleep(60)
            pass

        # When download finishes, close the Thread Pool and update the database
        self.update_local_info()

        # Restore the Sentinelsat messages and progress bars
        self.api.logger.disabled = False
        self.api.show_progressbars = True

    def download_all(self, max_attempts=10, background=False):
    
        # First we check if the database is ready to start downloading
        if not self.database.initialized(display_message=True):
            return

        # Then, even if the to_download list is empty, we will add the search_df to the database. That's usefull to start a database with images
        # already downloaded. There is a notebook to explain this workflow. No duplicates will be allowed. 
        if self._search_df is not None:
            self.database.add_data(self._search_df)

        # Then, we check if there is something that is not downloaded yet
        if self.to_download is None or len(self.to_download) == 0:
            self.logger.info('No images to download.')
            return

        # Regardless the download type, we will disable the messages from sentinelsat
        self.api.logger.disabled = True

        # if it is a background download, launches 2 threads, one for the download and the other to
        # check for its conclusion
        if background:
            print(f'Downloading {len(self.to_download)} images in the background.')
            print(f'To see the download list, access the .to_download attribute')
            print(f'To check the download status, use the .download_status() method.')

            # Turn off the sentinelsat messages and progress bars
            self.api.show_progressbars = False

            # Shutdown any existing executor
            if self.executor:
                self.executor.shutdown()

            self.executor = ThreadPoolExecutor(max_workers=2)
            self.result = self.executor.submit(self.api.download_all,
                                               self.to_download .index,
                                               directory_path=str(self.database.images_folder),
                                               max_attempts=max_attempts)

            # create a thread to monitor our download process
            self.monitor = self.executor.submit(self._monitor_download)

        # Otherwise, download directly
        else:
            self.status = self.api.download_all(self.to_download.index,
                                                directory_path=str(self.database.images_folder),
                                                max_attempts=max_attempts)

            self.update_local_info()
            self.api.logger.disabled = False

    def check_search_df(self, display_msg=False):
        if self._search_df is None or len(self._search_df) == 0:
            msg = f'Search dataset is empty. Please make a new search/query.'

            self.logger.info(msg) if display_msg else self.logger.debug(msg)

            return False
        else:
            return True

    # ------- PLOTTING METHODS -------- #
    def plot_quick_looks(self, db='search', query=None, cols=6):
        """
        Plot a grid of quick looks from a recent query (db=search) or from the Database.
        A query can be passed to filter the corresponding dataframe. If None is passed, plot all qlooks in the results
        :param db: Indicates the source of the quick looks. It can be either 'search' or 'database'
        :param query: A query string to filter the corresponding dataframe
        :param cols: Number of columns of the grid. The number of rows is calculated automatically
        """
        if db == 'search' and not self.check_search_df(display_msg=True):
            return

        if not self.database.initialized(display_message=True):
            return

        # get the correct dataframe
        df = self._search_df if db == 'search' else self.database.df

        # get the ids according to the given query (if any)
        ids = df.query(query).index if query is not None else df.index

        # if there is no image to display, just inform and quit
        if len(ids) == 0:
            print('No quick looks to display')
            return

        self.get_quick_looks()

        print('Title Format: TileId / Date / Is Online / Is Downloaded')

        # calc number of rows
        rows = math.ceil(len(ids) / cols)

        # create the file targets
        targets = df['title'].map(lambda x: self.database.qlooks_folder / (x + '.jpeg'))

        # create the figure
        fig, axs = plt.subplots(rows, cols, figsize=(25, 5 * rows))

        for i, idx in enumerate(ids):

            ax = axs.reshape(-1)[i]

            title = '/'.join(df.loc[idx, ['tile', 'date', 'online', 'downloaded']].astype('str'))
            ax.set_title(title)

            # check if the quick look file is there
            if not targets[idx].exists():
                ax.set_axis_off()

            # mark if not downloaded
            elif df.loc[idx, 'online'] and not df.loc[idx, 'downloaded']:
                self.color_axis(ax, color='red', width=3)

            # mark if not online and not downloaded
            elif not df.loc[idx, 'online'] and not df.loc[idx, 'downloaded']:
                self.color_axis(ax, color='orange', width=3)

            else:
                self.color_axis(ax, color='green', width=3)

            img = plt.imread(targets[idx])

            ax.imshow(img)

        # delete any unused axis
        for i in range(1, rows*cols-len(ids)+1):
            axs.reshape(-1)[-i].set_axis_off()

    # ------- STATIC METHODS -------- #
    @staticmethod
    def create_connection(user: str, password: str, api: str, logger=None):
        """
        Creates a SentinelAPI connection and tests if it is working correctly.
        :param user: username
        :param password: password
        :param api: api url
        :param logger: any logging context, or None use defaults.
        :return: Returns the SentinelAPI if it works or None if an exception is raised
        """

        # get the context logger
        logger = logging.getLogger('sentineldownloader') if logger is None else logger

        # create the SentinelSat api
        ssat = SentinelAPI(user, password, api_url=api)

        # set the same level of the current logger
        # ssat.logger.setLevel(logger.getEffectiveLevel())

        # after creating the api, request today's images just to check the connection
        try:
            logger.debug(f'Testing connection to: {api}')

            today = date.today().strftime('%Y%m%d')
            ssat.query(filename='*', date=(today, today))

            return ssat

        except Exception as e:

            logger.error(f"It was not possible to perform a query to {api}. "
                         f"Verify credentials and use the connect() method.")
            logger.error(e)

            return None

    @staticmethod
    def color_axis(ax, color, width=1):
        ax.set_xticks([])
        ax.set_xticklabels([])

        ax.set_yticks([])
        ax.set_yticklabels([])

        for axis in ['top', 'bottom', 'left', 'right']:
            ax.spines[axis].set_linewidth(width)
            ax.spines[axis].set_color(color)

    # ------- PROPERTIES -------- #
    @property
    def _check_api(self):
        if self.api is None:
            self.logger.info('No valid connection. Please use .connect() to establish a new connection')
            return False
        else:
            return True

    @property
    def _is_downloading(self):
        # check if there is a download in progress
        if self.result is not None and self.result.running():
            self.logger.info("Download in progress.")
            return True
        else:
            return False

    @property
    def to_download(self):
        """
        Return the list of images to be downloaded as a series. The list is composed by
        1- images in the query and not downloaded
        2- images in the database not downloaded (due to an error, for example)
        """
        
        # Don't show images to download if the database is not initialized
        if not self.database.initialized(display_message=True):
            return

        # If there is a database, use the combined_df (search + database) to check for possible downloads
        return self.combined_df[~self.combined_df['downloaded']]
        
        # to_download_search = self._search_df[~self._search_df['downloaded']]

        # concatenate the items from the search and the database
        # to_download = pd.concat([to_download_search, self.database.to_download])

        # Returns the list of images to be downloaded, removing any duplicates
        # return to_download[~to_download.index.duplicated()]

    @property
    def download_status(self):
        """
        Prints the download status.
        :return: None
        """
        if self.result is not None and self.result.running():
            progress = self.images_targets[self.to_download.index].map(Path.exists).astype('int')
            print(f'Downloading: {progress.sum()}/{len(progress)} completed.')
        else:
            print(f'No downloading process at the moment.')

        return

    @property
    def quick_looks_targets(self):
        if not self.check_search_df(display_msg=True) or not self.database.initialized(display_message=True):
            return

        return self.combined_df['title'].map(lambda x: self.database.qlooks_folder / (x + '.jpeg'))

    @property
    def combined_df(self):
        result = None
        if self.check_search_df(display_msg=False):
            result = self._search_df

        if self.database.initialized(display_message=False):
            if result is None:
                result = self.database.df
            else:
                result = pd.concat([result, self.database.df])

        return result[~result.index.duplicated()] if result is not None else None

    @property
    def images_targets(self):
        if not self.check_search_df() or not self.database.initialized():
            return

        return self.combined_df['title'].map(lambda x: self.database.images_folder / (x + '.zip'))

    @property
    def search_df(self):
        if self.check_search_df(display_msg=True):
            return self._search_df[SSatDownloader.base_columns]

    @property
    def database_df(self):
        if self.database.initialized(display_message=True):
            return self.database.df

    @property
    def summary(self):
        s = '---- Database --- \n'
        s += self.database.summary() + '\n'
        s += '---- SEARCH ---- \n'

        if self.check_search_df():
            if self.database.initialized():
                s += f"{len(self.search_df)} images found on server/ {self.search_df['online'].sum()} online / " \
                    f"{self.search_df['downloaded'].sum()} downloaded\n"
            else:
                s += f"{len(self.search_df)} images found on server/ {self.search_df['online'].sum()} online / " \
                    f"No download information. Open a database and run .update_local_info()\n"

            if self.result is not None and self.result.running():
                s += 'Downloading in the background'
        else:
            s += 'Empty search. Make a new query/search first.'
        return s

    # ------- OVERRIDDEN METHODS -------- #
    def __len__(self):
        return len(self.database)

    def __repr__(self):
        s = 'SSatDownloader class\n'
        return s + self.summary
