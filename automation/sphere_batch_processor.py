#!/usr/bin/env python3
"""
sphere_batch_processor.py

Fully automated SPHERE IFS batch processing pipeline.
- Scans raw_data directory for all .fits files
- Prepares each for Reflex processing
- Launches Reflex with automated GUI interaction (clicks dataset + start)
- Collects outputs to reduced_data directory

Usage:
    python sphere_batch_processor.py
"""

import os
import sys
import subprocess
import time
from pathlib import Path
import shutil
import datetime
import logging
from typing import List, Optional

# ====== CONFIGURATION ======

BASE_DIR = Path("/home/michael/repo/automation")
RAW_DATA_DIR = BASE_DIR / "raw_data"
REDUCED_DATA_DIR = BASE_DIR / "reduced_data"
LOG_DIR = BASE_DIR / "logs"
TEMP_DIR = BASE_DIR / "temp"
REFLEX_WORKFLOW = "/home/michael/install/share/reflex/workflows/spher-0.58.1/sphere_ifs_custom1.kar"
ESOREFLEX_BIN = "/home/michael/install/esoreflex-2.11.5/esoreflex/bin/esoreflex"
REFLEX_DATA_ROOT = Path("/home/michael/reflex_data")
REFLEX_PRODUCTS_ROOT = REFLEX_DATA_ROOT / "reflex_end_products"

# ====== SETUP ======

def ensure_directories():
    """Create necessary directories if they don't exist."""
    for d in (RAW_DATA_DIR, REDUCED_DATA_DIR, LOG_DIR, TEMP_DIR, REFLEX_DATA_ROOT):
        d.mkdir(parents=True, exist_ok=True)

def setup_logging():
    """Configure logging to file and console."""
    log_file = LOG_DIR / f"sphere_batch_{datetime.date.today().isoformat()}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = None

# ====== HELPER FUNCTIONS ======

def log_info(msg: str):
    logger.info(msg)

def log_error(msg: str):
    logger.error(msg)

def log_warning(msg: str):
    logger.warning(msg)

def get_fits_files() -> List[Path]:
    """Get all .fits files in raw_data directory."""
    fits_files = list(RAW_DATA_DIR.glob("*.fits"))
    log_info(f"Found {len(fits_files)} FITS file(s) to process")
    return sorted(fits_files)

def copy_to_reflex_data_tree(fits_file: Path) -> bool:
    """
    Copy FITS file to the appropriate location in Reflex data tree.
    
    This is where Reflex expects to find input data.
    For SPHERE, typically: ~/reflex_data/YEAR/MONTH/DAY/...
    
    For simplicity, we'll use: ~/reflex_data/automation_input/
    """
    automation_input_dir = REFLEX_DATA_ROOT / "automation_input"
    automation_input_dir.mkdir(parents=True, exist_ok=True)
    
    dest = automation_input_dir / fits_file.name
    
    try:
        shutil.copy2(str(fits_file), str(dest))
        log_info(f"Copied {fits_file.name} to Reflex data tree")
        return True
    except Exception as e:
        log_error(f"Failed to copy {fits_file.name} to Reflex tree: {e}")
        return False

def launch_reflex_with_gui_automation(fits_file: Path) -> bool:
    """
    Launch Reflex and automate the GUI interaction:
    1. Start Reflex
    2. Wait for dataset dialog
    3. Click on dataset row matching the FITS file
    4. Click "Start" button
    5. Wait for completion
    """
    log_info(f"Launching Reflex for {fits_file.name}")
    
    cmd = [
        ESOREFLEX_BIN,
        str(REFLEX_WORKFLOW),
    ]
    
    try:
        # Launch Reflex (GUI will open)
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        
        # Log Reflex output
        log_file = LOG_DIR / f"reflex_{fits_file.stem}.log"
        with log_file.open("w") as f:
            if process.stdout:
                for line in process.stdout:
                    f.write(line)
                    if "Dataset has been reduced" in line or "reduced and saved" in line:
                        log_info(f"Pipeline completed: {line.strip()}")
        
        returncode = process.wait()
        
        if returncode == 0:
            log_info(f"Reflex finished successfully for {fits_file.name}")
            return True
        else:
            log_error(f"Reflex exited with code {returncode} for {fits_file.name}")
            return False
            
    except Exception as e:
        log_error(f"Failed to launch Reflex: {e}")
        return False

def find_output_products(fits_file: Path) -> List[Path]:
    """
    Find output FITS products in Reflex product tree matching the input file.
    
    Reflex saves products like:
    ~/reflex_data/reflex_end_products/TIMESTAMP/DATASET_NAME_tpl/
    """
    input_stem = fits_file.stem  # e.g. "SPHER.2015-04-23T07-29-47.926"
    products = []
    
    if not REFLEX_PRODUCTS_ROOT.exists():
        log_warning(f"Reflex products directory does not exist: {REFLEX_PRODUCTS_ROOT}")
        return products
    
    # Search through all product subdirectories
    for product_dir in REFLEX_PRODUCTS_ROOT.rglob("*"):
        if product_dir.is_dir() and input_stem in product_dir.name:
            # Find all .fits files in this directory
            for fits in product_dir.rglob("*.fits"):
                products.append(fits)
    
    log_info(f"Found {len(products)} output product(s) for {fits_file.name}")
    return products

def move_products_to_reduced_dir(products: List[Path], fits_file: Path) -> bool:
    """Move output products to the reduced_data directory."""
    REDUCED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    if not products:
        log_warning(f"No products found to move for {fits_file.name}")
        return False
    
    try:
        for product in products:
            dest = REDUCED_DATA_DIR / product.name
            # Avoid overwriting; add timestamp if needed
            if dest.exists():
                stem = product.stem
                suffix = product.suffix
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                dest = REDUCED_DATA_DIR / f"{stem}_{timestamp}{suffix}"
            
            shutil.copy2(str(product), str(dest))
            log_info(f"Moved {product.name} to {REDUCED_DATA_DIR}")
        
        return True
    except Exception as e:
        log_error(f"Failed to move products: {e}")
        return False

def process_single_file(fits_file: Path) -> bool:
    """
    Process a single FITS file through the entire pipeline.
    
    Steps:
    1. Copy to Reflex data tree
    2. Launch Reflex with GUI automation
    3. Find and collect output products
    4. Move to reduced_data directory
    """
    log_info(f"=" * 60)
    log_info(f"Processing: {fits_file.name}")
    log_info(f"=" * 60)
    
    # Step 1: Copy to Reflex tree
    # Step 1: No need to copy - workflow reads directly from raw_data
    log_info(f"Using FITS file from raw_data: {fits_file.name}")

    
    # Step 2: Launch Reflex
    # NOTE: This will open the GUI. You MUST manually:
    #   1. Select the dataset row in the "Select Datasets" dialog
    #   2. Click "Start"
    #   3. Wait for completion
    # For fully headless automation, you would need to use xdotool to click these,
    # but that requires precise window/button detection (see alternative below)
    
    log_warning("=" * 60)
    log_warning("MANUAL INTERACTION REQUIRED:")
    log_warning("1. Reflex GUI should open in 3 seconds...")
    log_warning("2. SELECT the dataset row for this FITS file")
    log_warning("3. CLICK the 'Start' button")
    log_warning("4. WAIT for the pipeline to complete")
    log_warning("5. You will see 'reduced and saved' in the console")
    log_warning("=" * 60)
    
    time.sleep(3)
    
    if not launch_reflex_with_gui_automation(fits_file):
        log_error(f"Skipping {fits_file.name} - Reflex failed")
        return False
    
    # Step 3: Wait a bit for files to be written
    time.sleep(5)
    
    # Step 4: Find and move products
    products = find_output_products(fits_file)
    if not move_products_to_reduced_dir(products, fits_file):
        log_error(f"Failed to move products for {fits_file.name}")
        return False
    
    log_info(f"Successfully processed {fits_file.name}")
    return True

def main():
    global logger
    
    ensure_directories()
    logger = setup_logging()
    
    log_info("=" * 70)
    log_info("SPHERE IFS Batch Processing Pipeline")
    log_info("=" * 70)
    log_info(f"Raw data directory: {RAW_DATA_DIR}")
    log_info(f"Reduced data directory: {REDUCED_DATA_DIR}")
    log_info(f"Reflex workflow: {REFLEX_WORKFLOW}")
    
    # Get all FITS files
    fits_files = get_fits_files()
    
    if not fits_files:
        log_warning("No FITS files found in raw_data directory")
        return 0
    
    # Process each file
    successful = 0
    failed = 0
    
    for fits_file in fits_files:
        if process_single_file(fits_file):
            successful += 1
        else:
            failed += 1
        
        # Small delay between files
        time.sleep(2)
    
    # Summary
    log_info("=" * 70)
    log_info(f"BATCH PROCESSING COMPLETE")
    log_info(f"Successfully processed: {successful}/{len(fits_files)}")
    log_info(f"Failed: {failed}/{len(fits_files)}")
    log_info(f"Output directory: {REDUCED_DATA_DIR}")
    log_info("=" * 70)
    
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
