import logging
import pandas as pd
from pathlib import Path


class SSatDatabase:

    columns_dtypes ={'title': 'O',
                     'link': 'O',
                     'link_alternative': 'O',
                     'link_icon': 'O',
                     'summary': 'O',
                     'ondemand': 'O',
                     'beginposition': '<M8[ns]',
                     'endposition': '<M8[ns]',
                     'ingestiondate': '<M8[ns]',
                     'orbitnumber': 'int64',
                     'relativeorbitnumber': 'int64',
                     'cloudcoverpercentage': 'float64',
                     'highprobacloudspercentage': 'float64',
                     'mediumprobacloudspercentage': 'float64',
                     'notvegetatedpercentage': 'float64',
                     'snowicepercentage': 'float64',
                     'unclassifiedpercentage': 'float64',
                     'vegetationpercentage': 'float64',
                     'waterpercentage': 'float64',
                     'gmlfootprint': 'O',
                     'format': 'O',
                     'instrumentshortname': 'O',
                     'instrumentname': 'O',
                     'footprint': 'O',
                     's2datatakeid': 'O',
                     'platformidentifier': 'O',
                     'orbitdirection': 'O',
                     'platformserialidentifier': 'O',
                     'processingbaseline': 'O',
                     'processinglevel': 'O',
                     'producttype': 'O',
                     'platformname': 'O',
                     'size': 'O',
                     'filename': 'O',
                     'level1cpdiidentifier': 'O',
                     'identifier': 'O',
                     'uuid': 'O',
                     'datatakesensingstart': '<M8[ns]',
                     'sensoroperationalmode': 'O',
                     'tileid': 'O',
                     'hv_order_tileid': 'O',
                     'granuleidentifier': 'O',
                     'datastripidentifier': 'O',
                     'date': 'O',
                     'tile': 'O',
                     'online': 'bool',
                     'downloaded': 'bool',
                     'qlook': 'bool'}

    db_csv_name = 'download_db.csv'
    qlooks_dir = 'qlooks'
    images_dir = 'images'

    def __init__(self, downloader, logger_level=logging.INFO):

        self.logger = logging.getLogger('sentineldownloader.SSatDatabase')
        self.logger.setLevel(logger_level)

        self.downloader = downloader

        self.df = None
        self.folder = None

    def save_db(self):
        if self.df is not None:
            self.df.to_csv(self.folder / SSatDatabase.db_csv_name)

    def create(self, base_dir):
        self.folder = Path(base_dir)
        if self.folder.exists():
            self.logger.warning(f'Folder {base_dir} already exists. Cannot create new database.')
            self.folder = None
            self.df = None
            return False

        else:
            self.folder.mkdir(parents=True)

            (self.folder/self.qlooks_dir).mkdir(parents=True)
            (self.folder/self.images_dir).mkdir(parents=True)

            self.df = pd.DataFrame(columns=SSatDatabase.columns_dtypes.keys())

            # correct the data types
            self.df = self.df.astype(SSatDatabase.columns_dtypes)

            self.save_db()
            return True

    def open(self, base_dir):
        self.folder = Path(base_dir)
        images_folder = self.folder / self.images_dir
        qlooks_folder = self.folder / self.qlooks_dir

        if self.download_db_path.exists() and images_folder.exists() and qlooks_folder.exists():
            self.df = pd.read_csv(self.download_db_path, index_col='Unnamed: 0')
            self.df = self.df.astype(SSatDatabase.columns_dtypes, errors='ignore')
            
            self.update()
            return True
        else:
            self.logger.error(f'Folder {base_dir} if not a valid database.')
            self.folder = None
            self.df = None
            return False

    def summary(self):
        if self.initialized():
            if len(self) == 0:
                s = "Empty Database"
            else:
                s = f"{len(self)} images / {self.df['online'].sum()} online / " \
                    f"{self.df['downloaded'].sum()} downloaded \n"
        else:
            s = 'Database not initialized. Open or create a new database.'

        return s

    def initialized(self, display_message=False):
        if self.df is None and display_message:
            self.logger.info('Database is not initialized. Use open_database or create_database')

        return self.df is not None

    def add_data(self, df):
        if not self.initialized(display_message=True):
            return

        to_add = df[~df.index.isin(self.df.index)]
        self.df = pd.concat([self.df, to_add])

        # after adding the necessary data, save the database.
        self.save_db()

    def update(self):
        if self.initialized() and len(self) > 0:
            # Check if products are already downloaded in the output directory
            self.df['downloaded'] = self.images_targets.map(SSatDatabase.check_image)

            # Check if products are already in the output directory
            self.df['qlook'] = self.quick_looks_targets.map(Path.exists)

            # Remove duplicates
            self.df = self.df[~self.df.index.duplicated()]

            self.save_db()

    @property
    def download_db_path(self):
        if self.folder is not None:
            return self.folder/SSatDatabase.db_csv_name
        else:
            return None

    @property
    def quick_looks_targets(self):
        if not self.initialized():
            return

        return self.df['identifier'].map(lambda x: self.qlooks_folder / (x + '.jpeg'))

    @property
    def images_targets(self):
        if not self.initialized():
            return

        return self.df['identifier'].map(lambda x: self.images_folder / x)

    @property
    def to_download(self):
        if not self.initialized():
            return

        else:
            return self.df[~self.df['downloaded']]

    @property
    def images_folder(self):
        if self.initialized():
            return self.folder / self.images_dir
        else:
            return None

    @property
    def qlooks_folder(self):
        if self.initialized():
            return self.folder / self.qlooks_dir
        else:
            return None

    def __len__(self):
        return 0 if self.df is None else len(self.df)

    def __repr__(self):
        s = 'SSatDatabase class\n'
        s += self.summary()
        return s

    @staticmethod
    def check_image(path):
        path = Path(path)
        zip_file = path.with_suffix('.zip').exists()
        safe = path.with_suffix('.SAFE').exists()
        return zip_file or safe
