import asyncio
from concurrent.futures import ThreadPoolExecutor
from time import time, sleep
from multiprocessing import cpu_count
from types import coroutine
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
    def __init__(self) -> None:
        self.max_proc = None

    def __str__(self) -> str:
        return f"Qeue controller, {cpu_count()} CPUs are available on your machine"
    
    async def run_in_executor(self, executor: ThreadPoolExecutor, task_function: Callable, task_function_args: Tuple[Any], timeout: int) -> coroutine:
        """
        :param executor: ThreadPoolExecutor object from concurrent.futures
        :param task_function: target function
        :param task_function_args: task_function arguments
        :param timeout: process execution timeout in seconds
        :return: task coroutine
        """

        loop = asyncio.get_event_loop()
        try:
            result = await asyncio.wait_for(loop.run_in_executor(executor, task_function, *task_function_args), timeout=timeout)

            return result

        except asyncio.TimeoutError:
            print(f"The task #{(task_function, task_function_args)} time limit has been exceeded")

            return None

    def set_max_proc(self, n: int) -> None:
        """:param n: proc number"""
        max_cpu = cpu_count()

        if n > max_cpu:
            self.max_proc = max_cpu
            print("The limit has been exceeded, the maximum number of CPUs has been set")

        else:
            self.max_proc = n
            print(f"{n} CPUs are installed")

    async def start(self, tasks: TaskListType, max_exec_time: int):
        """
        :param tasks: list of tasks like [(function0, (f0_arg0, f0_arg1)), 
        (function1, (f1_arg0, f1_arg1, f1_arg2)), (function2, (f2_arg0, ...)), ...]
        :param max_exec_time: max task execution time in seconds
        """

        if self.max_proc is None:
            max_workers = cpu_count()

        else:
            max_workers = self.max_proc

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            tasks = [self.run_in_executor(executor, task[0], task[1], max_exec_time) for task in tasks]
            await asyncio.gather(*tasks)

    def wait(self):
        pass

    def wait_count(self):
        pass

    def alive_count(self):
        pass

def test(*args):
    start = time()
    sleep(args[0])
    stop = time()
    relation = stop - start

    if relation > 1:
        print(f"Success! Execution time: {relation}")

async def main():
    controller = ProcessController()
    tasks = [
        (test, (8, 2, 3)),
        (test, (3, 5, 5)),
        (test, (2,)),
        (test, (1, 2))
    ]
    await controller.start(tasks, 10)

if __name__ == "__main__":
    asyncio.run(main())
