import logging
import json
import os
from pathlib import Path
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)

class DataManager:
    """Handles pattern generation, checkpoint management, and data export"""
    
    def __init__(self):
        self.checkpoint_file = None
        self.shared_data_store = {
            "headers": ["Account Number", "Owner Name", "Mailing Address", "Property Address", "Legal Description"],
            "records": {},
            "processed_pages": set(),
            "last_saved_timestamp": None,
            "total_pages": 0,
            "total_records": 0,
            "search_patterns_completed": set(),
            "search_patterns_failed": set(),
            "search_patterns_no_results": set(),
            "current_search_pattern": None,
        }
        
        # Create checkpoint directory if it doesn't exist
        checkpoint_dir = Path("checkpoints")
        checkpoint_dir.mkdir(exist_ok=True)
    
    def generate_search_patterns(self):
        """Generate all search patterns (aaa%, aab%, etc.) - 3 letter patterns for Montgomery"""
        all_patterns = []
        for first_letter in "abcdefghijklmnopqrstuvwxyz":
            for second_letter in "abcdefghijklmnopqrstuvwxyz":
                for third_letter in "abcdefghijklmnopqrstuvwxyz":
                    pattern = f"{first_letter}{second_letter}{third_letter}%"
                    all_patterns.append(pattern)
        
        # Ensure patterns are sorted alphabetically
        all_patterns.sort()
        
        # Log the first few patterns to verify order
        if all_patterns:
            logger.info(f"First pattern: {all_patterns[0]}, Second: {all_patterns[1]}, Third: {all_patterns[2]}")
        
        logger.info(f"Generated {len(all_patterns)} total patterns")
        return all_patterns
    
    def load_completed_patterns(self):
        """Load completed patterns from existing checkpoints"""
        completed_patterns = set()
        failed_patterns = set()
        no_results_patterns = set()
        
        try:
            # Look for existing checkpoint files
            checkpoint_dir = Path("checkpoints")
            if checkpoint_dir.exists():
                checkpoint_files = list(checkpoint_dir.glob("montgomery_checkpoint_*.json"))
                
                if checkpoint_files:
                    # Use the most recent checkpoint
                    latest_checkpoint = max(checkpoint_files, key=os.path.getctime)
                    logger.info(f"Loading checkpoint from: {latest_checkpoint}")
                    
                    with open(latest_checkpoint, 'r', encoding='utf-8') as f:
                        checkpoint_data = json.load(f)
                    
                    # Load completed patterns
                    if "search_patterns_completed" in checkpoint_data:
                        completed_patterns = set(checkpoint_data["search_patterns_completed"])
                        logger.info(f"Loaded {len(completed_patterns)} completed patterns from checkpoint")
                    
                    # Load failed patterns
                    if "search_patterns_failed" in checkpoint_data:
                        failed_patterns = set(checkpoint_data["search_patterns_failed"])
                        logger.info(f"Loaded {len(failed_patterns)} failed patterns from checkpoint")
                    
                    # Load no results patterns
                    if "search_patterns_no_results" in checkpoint_data:
                        no_results_patterns = set(checkpoint_data["search_patterns_no_results"])
                        logger.info(f"Loaded {len(no_results_patterns)} no-results patterns from checkpoint")
                    
                    # Load other data
                    if "records" in checkpoint_data:
                        self.shared_data_store["records"] = checkpoint_data["records"]
                        logger.info(f"Loaded {len(self.shared_data_store['records'])} records from checkpoint")
                    
                    self.checkpoint_file = latest_checkpoint
                else:
                    logger.info("No existing checkpoint files found")
            else:
                logger.info("Checkpoint directory does not exist")
                
        except Exception as e:
            logger.error(f"Error loading checkpoint: {str(e)}")
        
        return completed_patterns, failed_patterns, no_results_patterns
    
    def save_checkpoint(self):
        """Save current progress to checkpoint file"""
        try:
            # Create checkpoint filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            checkpoint_filename = f"montgomery_checkpoint_{timestamp}.json"
            checkpoint_path = Path("checkpoints") / checkpoint_filename
            
            # Convert sets to lists for JSON serialization
            checkpoint_data = self.shared_data_store.copy()
            checkpoint_data["search_patterns_completed"] = list(checkpoint_data["search_patterns_completed"])
            checkpoint_data["search_patterns_failed"] = list(checkpoint_data["search_patterns_failed"])
            checkpoint_data["search_patterns_no_results"] = list(checkpoint_data["search_patterns_no_results"])
            checkpoint_data["processed_pages"] = list(checkpoint_data["processed_pages"])
            checkpoint_data["last_saved_timestamp"] = datetime.now().isoformat()
            
            # Save to file
            with open(checkpoint_path, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
            
            # Update checkpoint file reference
            self.checkpoint_file = checkpoint_path
            
            logger.info(f"Checkpoint saved: {checkpoint_path}")
            
            # Clean up old checkpoint files (keep only the 5 most recent)
            self._cleanup_old_checkpoints()
            
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")
    
    def _cleanup_old_checkpoints(self):
        """Clean up old checkpoint files, keeping only the 5 most recent"""
        try:
            checkpoint_dir = Path("checkpoints")
            checkpoint_files = list(checkpoint_dir.glob("montgomery_checkpoint_*.json"))
            
            if len(checkpoint_files) > 5:
                # Sort by creation time
                checkpoint_files.sort(key=os.path.getctime)
                
                # Remove oldest files
                files_to_remove = checkpoint_files[:-5]
                for file_path in files_to_remove:
                    try:
                        file_path.unlink()
                        logger.debug(f"Removed old checkpoint: {file_path}")
                    except Exception as e:
                        logger.warning(f"Could not remove old checkpoint {file_path}: {str(e)}")
                        
        except Exception as e:
            logger.warning(f"Error cleaning up old checkpoints: {str(e)}")
    
    def export_to_csv(self):
        """Export collected data to CSV file"""
        try:
            if not self.shared_data_store["records"]:
                logger.warning("No records to export")
                return
            
            # Convert records to DataFrame
            records_list = list(self.shared_data_store["records"].values())
            df = pd.DataFrame(records_list)
            
            # Ensure all required columns exist
            required_columns = ["Account_Number", "Owner_Name", "Mailing_Address", "Property_Address", "Legal_Description"]
            for col in required_columns:
                if col not in df.columns:
                    df[col] = "UNKNOWN"
            
            # Reorder columns
            df = df[required_columns]
            
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"montgomery_tax_results_{timestamp}.csv"
            
            # Ensure exports directory exists
            exports_dir = Path("exports")
            exports_dir.mkdir(exist_ok=True)
            
            filepath = exports_dir / filename
            
            # Export to CSV
            df.to_csv(filepath, index=False, encoding='utf-8')
            
            logger.info(f"Exported {len(df)} records to {filepath}")
            
            # Log summary statistics
            logger.info(f"Export summary:")
            logger.info(f"  - Total records: {len(df)}")
            logger.info(f"  - Unique account numbers: {df['Account_Number'].nunique()}")
            logger.info(f"  - Records with known owner names: {len(df[df['Owner_Name'] != 'UNKNOWN'])}")
            logger.info(f"  - Records with known mailing addresses: {len(df[df['Mailing_Address'] != 'UNKNOWN'])}")
            
        except Exception as e:
            logger.error(f"Error exporting to CSV: {str(e)}")
    
    def get_progress_summary(self):
        """Get a summary of current progress"""
        total_records = len(self.shared_data_store.get("records", {}))
        patterns_completed = len(self.shared_data_store.get("search_patterns_completed", set()))
        patterns_failed = len(self.shared_data_store.get("search_patterns_failed", set()))
        patterns_no_results = len(self.shared_data_store.get("search_patterns_no_results", set()))
        patterns_with_records = patterns_completed - patterns_no_results
        total_patterns_processed = patterns_completed + patterns_failed
        
        return {
            "total_records": total_records,
            "patterns_completed": patterns_completed,
            "patterns_failed": patterns_failed,
            "patterns_no_results": patterns_no_results,
            "patterns_with_records": patterns_with_records,
            "total_patterns_processed": total_patterns_processed
        } 