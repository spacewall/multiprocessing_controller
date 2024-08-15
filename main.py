import asyncio
from random import randint
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from time import time, sleep
from multiprocessing import Process, Queue, cpu_count
from types import coroutine
from typing import Callable, Dict, Iterable, Tuple, List, Any

from more_itertools import chunked

from upscaler import upscale

FunctionType = Callable[..., Any]
FunctionWithArgs = Tuple[FunctionType, Tuple[Any, ...]]
TaskListType = List[FunctionWithArgs]


class ProcessKillingExecutor:
    """
    The ProcessKillingExecutor works like an `Executor
    <https://docs.python.org/dev/library/concurrent.futures.html#executor-objects>`_
    in that it uses a bunch of processes to execute calls to a function with
    different arguments asynchronously.

    But other than the `ProcessPoolExecutor
    <https://docs.python.org/dev/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor>`_,
    the ProcessKillingExecutor forks a new Process for each function call that
    terminates after the function returns or if a timeout occurs.

    This means that contrary to the Executors and similar classes provided by
    the Python Standard Library, you can rely on the fact that a process will
    get killed if a timeout occurs and that absolutely no side can occur
    between function calls.

    Note that descendant processes of each process will not be terminated –
    they will simply become orphaned.
    """

    def __init__(self, max_workers: int=None):
        self.processes = max_workers or cpu_count()

    def map(self,
            func: Callable,
            iterable: Iterable,
            timeout: float=None,
            callback_timeout: Callable=None,
            daemon: bool = True
            ) -> Iterable:
        """
        :param func: the function to execute
        :param iterable: an iterable of function arguments
        :param timeout: after this time, the process executing the function
                will be killed if it did not finish
        :param callback_timeout: this function will be called, if the task
                times out. It gets the same arguments as the original function
        :param daemon: define the child process as daemon
        """
        executor = ThreadPoolExecutor(max_workers=self.processes)
        params = ({'func': func, 'fn_args': p_args, "p_kwargs": {},
                   'timeout': timeout, 'callback_timeout': callback_timeout,
                   'daemon': daemon} for p_args in iterable)
        return executor.map(self._submit_unpack_kwargs, params)

    def _submit_unpack_kwargs(self, params):
        """Unpack the kwargs and call submit"""

        return self.submit(**params)

    def submit(self,
               func: Callable,
               fn_args: Any,
               p_kwargs: Dict,
               timeout: float,
               callback_timeout: Callable[[Any], Any],
               daemon: bool):
        """
        Submits a callable to be executed with the given arguments.
        Schedules the callable to be executed as func(*args, **kwargs) in a new
         process.
        :param func: the function to execute
        :param fn_args: the arguments to pass to the function. Can be one argument
                or a tuple of multiple args.
        :param p_kwargs: the kwargs to pass to the function
        :param timeout: after this time, the process executing the function
                will be killed if it did not finish
        :param callback_timeout: this function will be called with the same
                arguments, if the task times out.
        :param daemon: run the child process as daemon
        :return: the result of the function, or None if the process failed or
                timed out
        """
        p_args = fn_args if isinstance(fn_args, tuple) else (fn_args,)
        queue = Queue()
        p = Process(target=self._process_run,
                    args=(queue, func, fn_args,), kwargs=p_kwargs)

        if daemon:
            p.deamon = True

        p.start()
        p.join(timeout=timeout)
        if not queue.empty():
            return queue.get()
        if callback_timeout:
            callback_timeout(*p_args, **p_kwargs)
        if p.is_alive():
            p.terminate()
            p.join()

    @staticmethod
    def _process_run(queue: Queue, func: Callable[[Any], Any]=None,
                     *args, **kwargs):
        """
        Executes the specified function as func(*args, **kwargs).
        The result will be stored in the shared dictionary
        :param func: the function to execute
        :param queue: a Queue
        """
        queue.put(func(*args, **kwargs))


class ProcessController:
    """
    This class presents some methods to controll
    task qeue execution in parallel mode
    """
    def __init__(self) -> None:
        self.max_proc = None
        self.counter = 0
        self.tasks_number = None

    def __str__(self) -> str:
        return f"Qeue controller, {cpu_count()} CPUs are available on your machine"
    
    async def run_in_executor(self, executor: ProcessPoolExecutor, task_function: Callable, task_function_args: Tuple[Any], timeout: int) -> coroutine:
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

    async def start(self, tasks: TaskListType, max_exec_time: int) -> None:
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
            if self.tasks_number <= max_workers:
                tasks_coroutines = [asyncio.create_task(self.run_in_executor(executor, task[0], task[1], max_exec_time)) for task in tasks]
            
                await asyncio.gather(*tasks_coroutines)

            else:
                chunked_tasks = chunked(tasks, self.max_proc)

                for tasks_chunk in chunked_tasks:
                    tasks_coroutines = [asyncio.create_task(self.run_in_executor(executor, task[0], task[1], max_exec_time)) for task in tasks_chunk]

                    await asyncio.gather(*tasks_coroutines)

    def wait(self) -> None:
        pass

    def wait_count(self) -> int:
        if self.tasks_number is not None:
            print(self.tasks_number, self.counter)
            return self.tasks_number - self.counter
        
        else:
            print(-1)
            return -1

    def alive_count(self):
        pass

def test(*args):
    start = time()
    sleep(args[0])
    stop = time()
    execution_time = stop - start

    if execution_time > 1:
        print(f"Success! Execution time: {execution_time}")

async def main():
    controller = ProcessController()
    # controller.set_max_proc(1)
    tasks = [(test, (8, randint(1, 10), 3)) for _ in range(25)]

    # asyncio.create_task(controller.start(tasks, 10))
    await controller.start(tasks, 10)

    start = time()
    # main_task = asyncio.current_task()
    # currents_tasks = asyncio.all_tasks()
    # currents_tasks.remove(main_task)
    # while time() - start < 4:
    #     pass

    # controller.wait_count()

if __name__ == "__main__":
    asyncio.run(main())
