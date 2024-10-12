import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SpeechUpdater:
    """
    This class is responsible for updating and saving the metadata of downloaded speeches.
    It loads existing metadata, merges with the new data, and updates the relevant files.
    """

    def __init__(self, metadata_file, backup_folder=None):
        """
        Initialize the SpeechUpdater with the path to the metadata file and optional backup folder.
        :param metadata_file: The path to the CSV file containing metadata.
        :param backup_folder: An optional folder to store backups of the metadata file.
        """
        self.metadata_file = metadata_file
        self.backup_folder = backup_folder

        # Ensure the backup folder exists
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
        """
        Create a backup of the existing metadata file.
        """
        if not self.backup_folder:
            return
        backup_file = os.path.join(self.backup_folder, f"metadata_backup_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv")
        self.metadata_df.to_csv(backup_file, index=False)
        logger.info(f"Backup of metadata created at {backup_file}")

    def merge_metadata(self, new_metadata):
        logger.info(f"Merging {len(new_metadata)} new metadata entries.")

        # 如果 new_metadata 为空，提前返回
        if not new_metadata:
            logger.info("No new metadata to merge. Skipping merge process.")
            return

        new_metadata_df = pd.DataFrame(new_metadata)

        # 检查 new_metadata_df 是否为空
        if new_metadata_df.empty:
            logger.info("New metadata DataFrame is empty. No data to merge.")
            return

        # 检查 'url' 列是否存在
        if 'url' not in new_metadata_df.columns:
            logger.error("The 'url' column is missing from the new metadata DataFrame.")
            logger.debug(f"New metadata DataFrame columns: {new_metadata_df.columns.tolist()}")
            return

        # 后续合并处理
        if not self.metadata_df.empty:
            existing_urls = set(self.metadata_df['url'])
            new_urls = set(new_metadata_df['url'])
            unique_new_urls = new_urls - existing_urls
            unique_new_metadata_df = new_metadata_df[new_metadata_df['url'].isin(unique_new_urls)]
            self.metadata_df = pd.concat([self.metadata_df, unique_new_metadata_df], ignore_index=True)
        else:
            self.metadata_df = new_metadata_df



    def save_metadata(self):
        """
        Save the merged metadata back to the CSV file.
        """
        logger.info(f"Saving metadata to {self.metadata_file}")
        self.metadata_df.to_csv(self.metadata_file, index=False)
        logger.info(f"Metadata saved successfully.")

    def update(self, new_metadata):
        if not new_metadata:
            logger.info("No new metadata provided. Skipping update.")
            return

        if self.backup_folder:
            self.backup_metadata()
        self.merge_metadata(new_metadata)
        self.save_metadata()
        logger.info("Metadata update completed.")

