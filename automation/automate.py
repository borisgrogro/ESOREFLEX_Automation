import os
import sys
import subprocess
from pathlib import Path
import shutil
import datetime

REFLEX_WORKFLOW = "/home/michael/install/share/reflex/workflows/spher-0.58.1/sphere_ifs_custom1.kar"
PIPELINE_OUTPUT_DIR = Path("/home/michael/automation/pipeline_products")
FINAL_REDUCED_DIR = Path("/home/michael/automation/reduced_data")
LOG_DIR = Path("/home/michael/automation/logs")

def append_log(message: str):
    msg = f"{datetime.datetime.now().isoformat()} {message}"
    print(msg)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with (LOG_DIR / "sphere_pipeline.log").open("a") as f:
        f.write(msg + "\n")

def move_outputs(input_fits_file: str):
    FINAL_REDUCED_DIR.mkdir(parents=True, exist_ok=True)
    input_name = Path(input_fits_file).stem
    moved = []
    for f in PIPELINE_OUTPUT_DIR.glob("*.fits"):
        if input_name in f.name:
            dest = FINAL_REDUCED_DIR / f.name
            shutil.move(str(f), dest)
            moved.append(dest)
    append_log(f"[move_outputs] Moved {len(moved)} file(s) for {input_fits_file}")

def run_pipeline(input_fits_file: str):
    input_fits_file = str(Path(input_fits_file).resolve())
    print(f"[run_pipeline] Called with: {input_fits_file}")
    print(f"[run_pipeline] CWD: {os.getcwd()}")


    cmd = [
    "/home/michael/install/esoreflex-2.11.5/esoreflex/bin/esoreflex",
    "/home/michael/install/share/reflex/workflows/spher-0.58.1/sphere_ifs_custom1.kar",
]



   
    append_log(f"[run_pipeline] Launching: {' '.join(cmd)}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    with (LOG_DIR / "sphere_pipeline.log").open("a") as logf:
        if process.stdout:
            for line in process.stdout:
                print(line, end="")
                logf.write(line)

    returncode = process.wait()
    append_log(f"[run_pipeline] Exit code: {returncode}")

    if returncode == 0:
        move_outputs(input_fits_file)
    else:
        append_log(f"[ERROR] Pipeline failed for {input_fits_file}")

def main():
    if len(sys.argv) != 2:
        print("Usage: run_sphere_pipeline.py <fits_file>")
        sys.exit(1)
    run_pipeline(sys.argv[1])

if __name__ == "__main__":
    main()
