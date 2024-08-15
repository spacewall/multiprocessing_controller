from time import time, sleep
from multiprocessing import Process, cpu_count
from typing import Callable, Tuple, List, Any

FunctionType = Callable[..., Any]
FunctionWithArgs = Tuple[FunctionType, Tuple[Any, ...]]
TaskListType = List[FunctionWithArgs]

from upscaler import upscale


class ProcessController:
    """
    This class presents some methods to controll
    task qeue execution in parallel mode
    """

    def __str__(self) -> str:
        return f"Qeue controller, {cpu_count()} CPUs are available on your machine"

    def set_max_proc(self, n: int) -> None:
        """:param n: proc number"""
        max_cpu = cpu_count()

        if n > max_cpu:
            self.max_proc = max_cpu
            print("The limit has been exceeded, the maximum number of CPUs has been set")

        else:
            self.max_proc = n
            print(f"{n} CPUs are installed")

    def start(self, tasks: TaskListType, max_exec_time):
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
    process.terminate()
    sleep(1)
    print(process.is_alive())
