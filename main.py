import os
from time import time, sleep
from multiprocessing import Process


class ProcessController:
    """…"""

    def __str__(self) -> str:
        pass

    def set_max_proc(self, n: int) -> None:
        self.max_proc = n

    def start(self):
        pass

    def wait(self):
        pass

    def wait_count(self):
        pass

    def alive_count(self):
        pass

# print(os.cpu_count())

def test():
    start = time()
    sleep(4)
    stop = time()

    if stop - start > 1:
        print("Success!")

if __name__ == "__main__":
    process = Process(target=test)
    process.start()

    print(process.is_alive())
    print(process.is_alive())
    sleep(10)
    print(process.is_alive())
