import pathlib
from loguru import logger
import subprocess
import os

STATUS_FILES = ["CopyComplete.txt", "RTAComplete.txt"]


class Nextseq():
    def __init__(self, machine_name, machine_path, backup_location):
        self.machine_name = machine_name
        self.machine_path = machine_path
        self.backup_location = backup_location

    @property
    def path_accessible(self):
        return os.path.ismount(self.machine_path)

    def get_sorted_folders(self) -> list:
        """
        return a list of sorted folders by modified time
        """
        sorted_folder = None
        if self.path_accessible:
            folders = [folder for folder in pathlib.Path(self.machine_path).iterdir(
            ) if folder.is_dir() and self.machine_name in folder.name]
            # Sort folders by modified time
            sorted_folder = sorted(
                folders, key=lambda d: d.stat().st_mtime, reverse=True)
        return sorted_folder

    @property
    def latest_run_path(self):
        latest = None
        _folders = self.get_sorted_folders()
        if _folders is not None and len(_folders) > 0:
            latest = _folders[0]
        return latest

    @property
    def latest_run_name(self):
        if self.latest_run_path is not None:
            return pathlib.Path(self.latest_run_path).name

    def copy(self):
        destination = pathlib.Path(self.backup_location).joinpath(
            self.latest_run_name)
        copy_cmd = f"rclone copy -P {self.latest_run_path} {destination}"
        logger.info(copy_cmd)
        subprocess.run(copy_cmd, shell=True, check=True)


class RunFolder():
    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.full_path = pathlib.Path(path).joinpath(name)

    @property
    def is_existed(self):
        return self.full_path.exists()

    @property
    def status_files_exists(self):
        status_files = False
        if self.is_existed:
            status_files = any([file for file in self.full_path.iterdir() if file.is_file() and file.name in STATUS_FILES])
        return status_files

    @property
    def is_finished(self):
        return self.status_files_exists

    @property
    def is_fully_copied(self):
        return self.status_files_exists

    def count_bgzf(self):
        """
        On Nextseq, each run usually has four lanes. We check number of bcl files in each lan, i.e. L001,L002,L003,L004.
        If number of files in each lane is the same, the copy is fine for starting off a bcl2fastq
        :run_name: Path to run
        """
        def _count(path):
            """
            Count everything inside a path folder using iterdir iterator.
            """
            counts = [run for run in path.iterdir() if run.suffix == ".bgzf"]
            return(len(counts))

        files_each_lane = []
        if self.is_existed:
            basecall_folder = self.full_path / "Data/Intensities/BaseCalls"
            for i in range(0, 4):
                _lane = basecall_folder / f"L00{i+1}"
                __count = None
                if _lane.exists():  # Some runs have lane folder (L00X) missing, so need to check before counting files inside that assumed lane folder
                    __count = _count(_lane)
                files_each_lane.append(__count)

        return(files_each_lane)
