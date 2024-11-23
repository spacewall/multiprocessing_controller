import asyncio
from concurrent.futures import ProcessPoolExecutor
from time import time, sleep
from multiprocessing import cpu_count
from types import coroutine
from typing import Callable, Tuple, List, Any

from more_itertools import chunked

FunctionType = Callable[..., Any]
FunctionWithArgs = Tuple[FunctionType, Tuple[Any, ...]]
TaskListType = List[FunctionWithArgs]


class ProcessController:
    """
    This class presents some methods to controll
    task qeue execution in parallel mode
    """
    def __init__(self) -> None:
        self.max_proc = None
        self.counter = 0
        self.alive_tasks = 0
        self.tasks_number = None
        self.waiter = False

    def __str__(self) -> str:
        return f"Qeue controller, {cpu_count()} CPUs are available on your machine"
    
    async def _run_in_executor(self, executor: ProcessPoolExecutor, task_function: Callable, task_function_args: Tuple[Any], timeout: int) -> coroutine:
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
            self.counter += 1

            return result

        except asyncio.TimeoutError:
            print(f"The task #{(task_function, task_function_args)} time limit has been exceeded")
            # executor.shutdown(wait=True)

            return None

    def set_max_proc(self, n: int) -> None:
        """
        Set max number of processes
        :param n: proc number
        """
        max_cpu = cpu_count()

        if n > max_cpu:
            self.max_proc = max_cpu
            print("The limit has been exceeded, the maximum number of CPUs has been set")

        else:
            self.max_proc = n
            print(f"{n} CPUs are installed")

    async def _start_handler(self, tasks: TaskListType, max_exec_time: int) -> None:
        """
        :param tasks: list of tasks like [(function0, (f0_arg0, f0_arg1)), 
        (function1, (f1_arg0, f1_arg1, f1_arg2)), (function2, (f2_arg0, ...)), ...]
        :param max_exec_time: max task execution time in seconds
        """

        if self.max_proc is None:
            max_workers = cpu_count()

        else:
            max_workers = self.max_proc

        self.tasks_number = len(tasks)

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            if self.tasks_number < max_workers:
                print(len(tasks))
                tasks_coroutines = [asyncio.create_task(self._run_in_executor(executor, task[0], task[1], max_exec_time)) for task in tasks]
            
                self.alive_tasks = len(tasks_coroutines)
                await asyncio.gather(*tasks_coroutines)

            else:
                chunked_tasks = chunked(tasks, self.max_proc)

                for tasks_chunk in chunked_tasks:
                    tasks_coroutines = [asyncio.create_task(self._run_in_executor(executor, task[0], task[1], max_exec_time)) for task in tasks_chunk]

                    self.alive_tasks = len(tasks_coroutines)
                    await asyncio.gather(*tasks_coroutines)                 

    async def start(self, tasks: TaskListType, max_exec_time: int) -> None:
        """
        Start computations in parallel mode
        :param tasks: list of tasks like [(function0, (f0_arg0, f0_arg1)), 
        (function1, (f1_arg0, f1_arg1, f1_arg2)), (function2, (f2_arg0, ...)), ...]
        :param max_exec_time: max task execution time in seconds
        """
        main_task = asyncio.current_task()
        currents_tasks = asyncio.all_tasks()
        currents_tasks.remove(main_task)

        if self.waiter:
            await self._start_handler(tasks, max_exec_time)

        else:
            asyncio.create_task(self._start_handler(tasks, max_exec_time))

        self.alive_tasks = 0
    
    def wait(self) -> bool:
        """Wait until all tasks will be execute"""
        self.waiter = True

    def wait_count(self) -> Any:
        """Return number of tasks in event loop"""
        if self.tasks_number is not None:
            return self.tasks_number - self.counter
        
        else:
            print("Here is not any computations, execute start function first")
            return None

    def alive_count(self) -> int:
        """Return number of tasks in progress"""
        print(self.alive_tasks)
        return self.alive_tasks


def test(*args):
    start = time()
    sleep(args[0])
    stop = time()
    execution_time = stop - start

    if execution_time > 1:
        print(f"Success! Execution time: {execution_time}")

async def main() -> None:
    controller = ProcessController()
    controller.set_max_proc(3)
    tasks = [(test, (2,)) for _ in range(11)]

    controller.wait()
    await controller.start(tasks, 10)
    await asyncio.sleep(1.5)
    controller.wait_count()
    controller.alive_count()

if __name__ == "__main__":
    asyncio.run(main())
