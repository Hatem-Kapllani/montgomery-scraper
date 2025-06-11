import json
import os
import time
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

def save_checkpoint(data_store):
    """Save checkpoint data to a JSON file"""
    try:
        checkpoint_dir = Path("checkpoints")
        checkpoint_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        checkpoint_file = checkpoint_dir / f"galveston_checkpoint_{timestamp}.json"
        
        # Add timestamp to data store
        data_store["last_saved_timestamp"] = datetime.now().isoformat()
        
        # Convert sets to lists for JSON serialization
        json_data = {}
        for key, value in data_store.items():
            if isinstance(value, set):
                json_data[key] = list(value)
            else:
                json_data[key] = value
        
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Checkpoint saved: {checkpoint_file}")
        return str(checkpoint_file)
        
    except Exception as e:
        logger.error(f"Error saving checkpoint: {str(e)}")
        return None

def load_latest_checkpoint():
    """Load the most recent checkpoint file"""
    try:
        checkpoint_dir = Path("checkpoints")
        if not checkpoint_dir.exists():
            logger.info("No checkpoint directory found")
            return {}, None
        
        # Find all Galveston checkpoint files
        checkpoint_files = list(checkpoint_dir.glob("galveston_checkpoint_*.json"))
        
        if not checkpoint_files:
            logger.info("No Galveston checkpoint files found")
            return {}, None
        
        # Verify files actually exist (not just path objects)
        existing_files = [f for f in checkpoint_files if f.exists() and f.is_file() and f.stat().st_size > 0]
        
        if not existing_files:
            logger.info("No valid Galveston checkpoint files found")
            return {}, None
            
        # Sort by modification time and get the latest
        latest_checkpoint = max(existing_files, key=lambda x: x.stat().st_mtime)
        
        logger.info(f"Loading checkpoint: {latest_checkpoint}")
        
        with open(latest_checkpoint, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Convert lists back to sets where appropriate
        if "search_patterns_completed" in data and isinstance(data["search_patterns_completed"], list):
            data["search_patterns_completed"] = set(data["search_patterns_completed"])
        
        if "processed_pages" in data and isinstance(data["processed_pages"], list):
            data["processed_pages"] = set(data["processed_pages"])
        
        logger.info(f"Loaded checkpoint with {len(data.get('records', {}))} records")
        return data, str(latest_checkpoint)
        
    except Exception as e:
        logger.error(f"Error loading checkpoint: {str(e)}")
        return {}, None

def cleanup_old_checkpoints(keep_count=10):
    """Keep only the most recent checkpoint files"""
    try:
        checkpoint_dir = Path("checkpoints")
        if not checkpoint_dir.exists():
            return
        
        checkpoint_files = list(checkpoint_dir.glob("galveston_checkpoint_*.json"))
        
        if len(checkpoint_files) <= keep_count:
            return
        
        # Sort by modification time
        checkpoint_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        # Remove old files
        for old_file in checkpoint_files[keep_count:]:
            try:
                old_file.unlink()
                logger.info(f"Removed old checkpoint: {old_file}")
            except Exception as e:
                logger.warning(f"Could not remove old checkpoint {old_file}: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error cleaning up checkpoints: {str(e)}") 

def export_to_csv(data_store, output_prefix="galveston_tax"):
    """Export data store records to CSV"""
    try:
        import pandas as pd
        from datetime import datetime
        
        # Convert records to list for DataFrame
        records_list = []
        for key, record in data_store.get("records", {}).items():
            records_list.append(record)
        
        if not records_list:
            logger.warning("No records to export")
            return None
        
        # Create DataFrame
        df = pd.DataFrame(records_list)
        
        # Create output directory if it doesn't exist
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = output_dir / f"{output_prefix}_{timestamp}.csv"
        
        # Save to CSV
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        logger.info(f"Results exported to {csv_path}")
        logger.info(f"Total records exported: {len(records_list)}")
        return str(csv_path)
            
    except Exception as e:
        logger.error(f"Error exporting to CSV: {str(e)}")
        return None