
from tq.taskqueue import add, add_task, Task
from tq.worker import Worker

__version__ = '0.1.0'
VERSION = tuple(map(int, __version__.split('.')))
