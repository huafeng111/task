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
        if backup_folder:
            if not os.path.exists(backup_folder):
                os.makedirs(backup_folder)
                logger.debug(f"Created backup folder: {backup_folder}")

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
            logger.info("No backup folder specified. Skipping backup.")
            return

        # Create a backup with a unique name based on the current timestamp
        backup_file = os.path.join(self.backup_folder, f"metadata_backup_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv")
        self.metadata_df.to_csv(backup_file, index=False)
        logger.info(f"Backup of metadata created at {backup_file}")

    def merge_metadata(self, new_metadata):
        """
        Merge new metadata with the existing metadata.
        :param new_metadata: A list of dictionaries containing the new metadata.
        """
        logger.info(f"Merging {len(new_metadata)} new metadata entries.")
        new_metadata_df = pd.DataFrame(new_metadata)

        # Merge the new metadata with existing data, avoiding duplicates
        if not self.metadata_df.empty:
            combined_metadata = pd.concat([self.metadata_df, new_metadata_df], ignore_index=True)
            # Drop duplicates based on key fields (like 'url' or 'file_path')
            combined_metadata.drop_duplicates(subset=['url'], keep='last', inplace=True)
            self.metadata_df = combined_metadata
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
        """
        The main update function that handles merging, backing up, and saving new metadata.
        :param new_metadata: A list of dictionaries containing the new metadata.
        """
        if self.backup_folder:
            self.backup_metadata()

        self.merge_metadata(new_metadata)
        self.save_metadata()
        logger.info("Metadata update completed.")


# Example usage
if __name__ == "__main__":
    metadata_file = '../data/pdfs/speech_metadata.csv'
    backup_folder = '../data/pdfs/backup_metadata'

    updater = SpeechUpdater(metadata_file=metadata_file, backup_folder=backup_folder)

    # Example new metadata (usually this will come from the SpeechDownloader class)
    new_metadata = [
        {'url': 'https://example.com/speech1.pdf', 'year': 2024, 'title': 'Monetary Policy Update', 'author': 'Jerome Powell', 'date': '2024-01-15', 'file_path': '../data/pdfs/speech1.pdf'},
        {'url': 'https://example.com/speech2.pdf', 'year': 2024, 'title': 'Economic Outlook', 'author': 'Lael Brainard', 'date': '2024-02-01', 'file_path': '../data/pdfs/speech2.pdf'}
    ]

    # Update the metadata with the new entries
    updater.update(new_metadata)
