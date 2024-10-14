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
        # Check if metadata_file is a valid string
        if not isinstance(metadata_file, str) or not metadata_file.endswith('.csv'):
            logger.error("The metadata_file parameter must be a valid CSV file path.")
            raise ValueError("The metadata_file parameter must be a valid CSV file path.")

        self.metadata_file = metadata_file
        self.backup_folder = backup_folder

        # Ensure the backup folder exists if provided
        if backup_folder:
            if not isinstance(backup_folder, str):
                logger.error("The backup_folder parameter must be a valid directory path.")
                raise ValueError("The backup_folder parameter must be a valid directory path.")
            if not os.path.exists(backup_folder):
                try:
                    os.makedirs(backup_folder)
                    logger.info(f"Backup folder created at {backup_folder}")
                except OSError as e:
                    logger.error(f"Failed to create backup folder at {backup_folder}: {e}")
                    raise

        # Load existing metadata if available
        if os.path.exists(self.metadata_file):
            try:
                logger.info(f"Loading existing metadata from {self.metadata_file}")
                self.metadata_df = pd.read_csv(self.metadata_file)
            except pd.errors.EmptyDataError:
                logger.warning(f"The metadata file {self.metadata_file} is empty. Starting with an empty DataFrame.")
                self.metadata_df = pd.DataFrame()
            except Exception as e:
                logger.error(f"Failed to load metadata from {self.metadata_file}: {e}")
                raise
        else:
            logger.info(f"No existing metadata found. Starting fresh.")
            self.metadata_df = pd.DataFrame()

    def backup_metadata(self):
        """
        Create a backup of the existing metadata file.
        """
        if not self.backup_folder:
            logger.warning("Backup folder is not specified. Skipping backup.")
            return
        try:
            backup_file = os.path.join(self.backup_folder, f"metadata_backup_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv")
            self.metadata_df.to_csv(backup_file, index=False)
            logger.info(f"Backup of metadata created at {backup_file}")
        except Exception as e:
            logger.error(f"Failed to create a backup of the metadata: {e}")
            raise

    def merge_metadata(self, new_metadata):
        """
        Merge new metadata with the existing metadata.
        :param new_metadata: A list of dictionaries containing new metadata entries.
        """
        if not isinstance(new_metadata, list):
            logger.error("New metadata must be provided as a list of dictionaries.")
            raise ValueError("New metadata must be provided as a list of dictionaries.")

        logger.info(f"Merging {len(new_metadata)} new metadata entries.")

        if not new_metadata:
            logger.info("No new metadata to merge. Skipping merge process.")
            return

        try:
            new_metadata_df = pd.DataFrame(new_metadata)
        except ValueError as e:
            logger.error(f"Failed to create DataFrame from new metadata: {e}")
            raise

        if new_metadata_df.empty:
            logger.info("New metadata DataFrame is empty. No data to merge.")
            return

        if 'url' not in new_metadata_df.columns:
            logger.error("The 'url' column is missing from the new metadata DataFrame.")
            logger.debug(f"New metadata DataFrame columns: {new_metadata_df.columns.tolist()}")
            raise KeyError("The 'url' column is required in the new metadata.")

        # Proceed with merging
        try:
            if not self.metadata_df.empty:
                existing_urls = set(self.metadata_df['url'])
                new_urls = set(new_metadata_df['url'])
                unique_new_urls = new_urls - existing_urls
                unique_new_metadata_df = new_metadata_df[new_metadata_df['url'].isin(unique_new_urls)]
                self.metadata_df = pd.concat([self.metadata_df, unique_new_metadata_df], ignore_index=True)
            else:
                self.metadata_df = new_metadata_df
        except Exception as e:
            logger.error(f"Failed during metadata merge: {e}")
            raise

    def save_metadata(self):
        """
        Save the merged metadata back to the CSV file.
        """
        try:
            logger.info(f"Saving metadata to {self.metadata_file}")
            self.metadata_df.to_csv(self.metadata_file, index=False)
            logger.info(f"Metadata saved successfully.")
        except Exception as e:
            logger.error(f"Failed to save metadata to {self.metadata_file}: {e}")
            raise

    def update(self, new_metadata):
        """
        Update the metadata with new entries, including creating backups and saving.
        :param new_metadata: A list of dictionaries containing new metadata entries.
        """
        if not isinstance(new_metadata, list):
            logger.error("New metadata must be provided as a list of dictionaries.")
            raise ValueError("New metadata must be provided as a list of dictionaries.")

        if not new_metadata:
            logger.info("No new metadata provided. Skipping update.")
            return

        if self.backup_folder:
            self.backup_metadata()
        self.merge_metadata(new_metadata)
        self.save_metadata()
        logger.info("Metadata update completed.")