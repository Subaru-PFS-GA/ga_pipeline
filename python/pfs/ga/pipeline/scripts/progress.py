import os
from tqdm import tqdm

from ..setup_logger import logger

class Progress():
    """
    Mixin class to implement a progress bar, dry-run and top, etc.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.__dry_run = False              # Dry run mode
        self.__top = None                   # Stop after this many objects
        self.__progress = False             # Display a progress bar

    def __get_dry_run(self):
        return self.__dry_run
    
    dry_run = property(__get_dry_run)

    def __get_top(self):
        return self.__top
    
    top = property(__get_top)

    def __get_progress(self):
        return self.__progress
    
    progress = property(__get_progress)

    def _add_args(self):           
        self.add_arg('--dry-run', action='store_true', help='Dry run mode')
        self.add_arg('--top', type=int, help='Stop after this many objects')
        self.add_arg('--progress', action='store_true', help='Display a progress bar.')

    def _init_from_args(self, args):
        self.__dry_run = self.get_arg('dry_run', args, self.__dry_run)
        self.__top = self.get_arg('top', args, self.__top)
        self.__progress = self.get_arg('progress', args, self.__progress)

    def _wrap_in_progressbar(self, iterable, total=None):
        return tqdm(iterable, total=total)