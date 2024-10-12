# C:\Users\admin\Documents\GitHub\task\src\core\BaseUpdater.py
import os
import pandas as pd
import logging

# Configure logging for base updater
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BaseUpdater:
    def __init__(self, metadata_file, backup_folder=None):
        self.metadata_file = metadata_file
        self.backup_folder = backup_folder

        if backup_folder and not os.path.exists(backup_folder):
            os.makedirs(backup_folder)

        # Load existing metadata if available
        if os.path.exists(self.metadata_file):
            logger.info(f"Loading existing metadata from {self.metadata_file}")
            self.metadata_df = pd.read_csv(self.metadata_file)
        else:
            logger.info(f"No existing metadata found. Starting fresh.")
            self.metadata_df = pd.DataFrame()

    def backup_metadata(self):
        if not self.backup_folder:
            return
        backup_file = os.path.join(self.backup_folder, f"metadata_backup_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv")
        self.metadata_df.to_csv(backup_file, index=False)
        logger.info(f"Backup of metadata created at {backup_file}")

    def merge_metadata(self, new_metadata):
        logger.info(f"Merging {len(new_metadata)} new metadata entries.")
        new_metadata_df = pd.DataFrame(new_metadata)

        if not self.metadata_df.empty:
            existing_urls = set(self.metadata_df['url'])
            new_urls = set(new_metadata_df['url'])
            unique_new_urls = new_urls - existing_urls
            unique_new_metadata_df = new_metadata_df[new_metadata_df['url'].isin(unique_new_urls)]
            self.metadata_df = pd.concat([self.metadata_df, unique_new_metadata_df], ignore_index=True)
        else:
            self.metadata_df = new_metadata_df

    def save_metadata(self):
        logger.info(f"Saving metadata to {self.metadata_file}")
        self.metadata_df.to_csv(self.metadata_file, index=False)
        logger.info(f"Metadata saved successfully.")

    def update(self, new_metadata):
        if self.backup_folder:
            self.backup_metadata()
        self.merge_metadata(new_metadata)
        self.save_metadata()
        logger.info("Metadata update completed.")
