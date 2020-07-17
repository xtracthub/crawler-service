
import time
from queue import Queue
from crawlers.base import Crawler
from funcx import FuncXClient
import threading


fxc = FuncXClient()
fxc.throttling_enabled = False


class LocalFuncXCrawler(Crawler):

    def __init__(self, crawl_id, start_dir, fx_eid="7dd05518-c6ca-4bf8-ad2c-801adaa61da7"):
        Crawler.__init__(self)
        self.crawl_id = crawl_id
        self.start_dir = start_dir
        self.dirs_to_crawl = Queue()
        self.fx_eid = fx_eid
        self.start_time = time.time()
        self.end_time = None
        self.total_time = None
        self.crawl_status = "STARTING"

        self.dirs_to_crawl.put(self.start_dir)
        self.funcx_task_q = Queue()
        self.metadata_ls = []

        self.no_tasks_bool = False
        self.no_dirs_bool = False

        self.function_id = fxc.register_function(get_metadata, description="Crawler for local FS")

    def crawl(self):
        launch_thread = threading.Thread(target=self.funcx_launch, args=())
        launch_thread.start()

        poll_thread = threading.Thread(target=self.funcx_poll, args=())
        poll_thread.start()

        launch_thread.join()
        poll_thread.join()

        print(self.metadata_ls)
        print(len(self.metadata_ls))
        print(self.dirs_to_crawl.qsize())
        print(self.funcx_task_q.qsize())

    def funcx_launch(self):
        print("Welcome to the launch thread!")

        while True:
            if self.no_tasks_bool and self.no_dirs_bool and self.funcx_task_q.empty() \
                    and self.dirs_to_crawl.empty():
                return 0  # Just return to join the thread.

            if not self.dirs_to_crawl.empty():
                self.no_dirs_bool = False

                i = 0
                new_dir_ls = []
                while not self.dirs_to_crawl.empty() and i < 1000:  # TODO: arg
                    new_dir_ls.append(self.dirs_to_crawl.get())

                task_uuid = fxc.run(new_dir_ls, endpoint_id=self.fx_eid, function_id=self.function_id)
                self.funcx_task_q.put(task_uuid)
            else:
                print(f"No dirs in queue! Sleeping...")
                self.no_dirs_bool = True
                time.sleep(2)

    def funcx_poll(self):
        error_counter = 0
        success_counter = 0
        while True:

            if self.no_tasks_bool and self.no_dirs_bool \
                    and self.funcx_task_q.empty() and self.dirs_to_crawl.empty():
                print("Poll thread exiting!")
                return 0  # Just return to join the thread.

            if not self.funcx_task_q.empty():
                print("Task queue is not empty! ")
                self.no_tasks_bool = False

                task_id = self.funcx_task_q.get()
                print(f"Checking task ID: {task_id}")

                status_obj = fxc.get_task(task_id)
                print(status_obj)
                if 'result' in status_obj:

                    # This is
                    result = status_obj['result']
                    next_dirs = result['next_dirs']
                    metadata = result['metadata']

                    for dir_path in next_dirs:
                        self.dirs_to_crawl.put(dir_path)
                    for f_mdata in metadata:
                        self.metadata_ls.append(f_mdata)

                    success_counter += 1
                    print(f"Num. Successes: {success_counter}")

                elif 'exception' in status_obj:
                    print(f"Exception: {status_obj['exception']}")
                    error_counter += 1
                    print(f"Error count: {error_counter}")
                    status_obj['exception'].reraise()

                else:
                    print("Status is pending!")
                    self.funcx_task_q.put(task_id)
                    time.sleep(1)
            else:
                self.no_tasks_bool = True
                time.sleep(2)


def get_metadata(dirs_to_process):
    """Crawl local filesystem. Return state (i.e, file list)
        :param directory string representing root level directory path """
    import os
    import magic

    f = magic.Magic(mime=True, uncompress=True)

    # TODO: literal copy/paste from globus_base. Bump out to utils!
    def get_extension(filepath):
        """Returns the extension of a filepath.
        Parameter:
        filepath (str): Filepath to get extension of.
        Return:
        extension (str): Extension of filepath.
        """
        filename = filepath.split('/')[-1]
        extension = None

        if '.' in filename:
            extension = filename.split('.')[-1]
        return extension

    next_dirs = []
    mdata_to_return = []

    for dir_to_crawl in dirs_to_process:
        dir_ls = os.listdir(dir_to_crawl)

        for item in dir_ls:

            full_path = os.path.join(dir_to_crawl, item)

            if os.path.isdir(full_path):
                next_dirs.append(full_path)

            # Is otherwise a file
            else:
                file_size = os.stat(full_path).st_size
                extension = get_extension(full_path)
                try:
                    mimeType = f.from_file(full_path)
                except PermissionError as pe:
                    continue

                data_to_group = {"full_path": full_path, "file_size": file_size, "extension": extension, "mimeType": mimeType}
                mdata_to_return.append(data_to_group)
    return {'metadata': mdata_to_return, 'next_dirs': next_dirs}

import os
def get_metadata2(directory):
    """Crawl local filesystem. Return state (i.e, file list)
        :param directory string representing root level directory path """
    pass
    r = []
    subdirs = [x[0] for x in os.walk(directory)]
    for subdir in subdirs:
        files = os.walk(subdir).__next__()[2]
        if len(files) > 0:
            for item in files:
                file_path = subdir + "/" + item
                size = os.stat(file_path).st_size
                r.append(5)

    return r


t0 = time.time()
x1 = LocalFuncXCrawler(crawl_id="0", start_dir='/Users/tylerskluzacek/Desktop')
x1.crawl()
t1 = time.time()

print(t1-t0)

t2 = time.time()
x = get_metadata2('/Users/tylerskluzacek/Desktop')
print(len(x))
t3 = time.time()

print(t3-t2)