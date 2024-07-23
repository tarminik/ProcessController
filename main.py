import multiprocessing as mp
import time


class ProcessController:
    def __init__(self):
        self.max_proc = 1
        self.task_queue = mp.Queue()
        self.current_processes = []
        self.manager = mp.Manager()
        self.active_tasks = self.manager.list()

    def set_max_proc(self, n):
        self.max_proc = n

    def worker(self, func, args, max_exec_time, stop_event):
        process = mp.Process(target=self.run_with_timeout, args=(func, args, max_exec_time, stop_event))
        process.start()
        process.join()

    def run_with_timeout(self, func, args, max_exec_time, stop_event):
        def target_wrapper(stop_event):
            result = func(*args)
            if stop_event.is_set():
                return
            print(f"Задание {func.__name__} с аргументами {args} завершено с результатом: {result}")

        p = mp.Process(target=target_wrapper, args=(stop_event,))
        p.start()
        p.join(max_exec_time)

        if p.is_alive():
            print(f"Задание {func.__name__} с аргументами {args} превысило "
                  f"лимит времени {max_exec_time} секунд и будет завершено.")
            stop_event.set()
            p.terminate()
            p.join()

        self.active_tasks.remove(func)

    def start(self, tasks, max_exec_time):
        """
        :param tasks: список заданий. Задание представляет кортеж вида: (функция, кортеж входных аргументов для функции).
        :param max_exec_time: максимальное время (в секундах) работы каждого задания из списка tasks.
        :return:
        """
        for func, args in tasks:
            self.task_queue.put((func, args, max_exec_time))

        self._process_tasks()

    def _process_tasks(self):
        while self.current_processes or not self.task_queue.empty():
            while not self.task_queue.empty() and len(self.current_processes) < self.max_proc:
                func, args, max_exec_time = self.task_queue.get()
                stop_event = mp.Event()
                self.active_tasks.append(func)
                p = mp.Process(target=self.worker, args=(func, args, max_exec_time, stop_event))
                self.current_processes.append(p)
                p.start()
            self._cleanup_processes()
            time.sleep(0.1)
            # self.print_status()

    def _cleanup_processes(self):
        self.current_processes = [p for p in self.current_processes if p.is_alive()]

    def print_status(self):
        print(f"Активные задания: {self.alive_count()}")
        print(f"Задания в очереди: {self.wait_count()}")

    def wait(self):
        """
        Ждет пока не завершат своё выполнение все задания из очереди.
        :return:
        """
        for p in self.current_processes:
            p.join()

    def wait_count(self):
        """
        :return: число заданий, которые осталось запустить.
        """
        return self.task_queue.qsize()

    def alive_count(self):
        """
        :return: число выполняемых в данный момент заданий.
        """
        return len([p for p in self.current_processes if p.is_alive()])


# Пример использования класса ProcessController
def example_task(duration, name):
    time.sleep(duration)
    return f"Задание {name} завершено"


def compute_sum(a, b):
    time.sleep(2)
    return a + b


def compute_product(a, b):
    time.sleep(3)
    return a * b


def long_task(duration):
    time.sleep(duration)
    return f"Длинное задание завершено за {duration} секунд"


if __name__ == "__main__":
    tasks = [
        (example_task, (2, "A")),
        (example_task, (4, "B")),
        (example_task, (1, "C")),
        (example_task, (3, "D")),
        (compute_sum, (5, 10)),
        (compute_product, (3, 7)),
        (long_task, (6,))
    ]

    controller = ProcessController()
    controller.set_max_proc(3)
    controller.start(tasks, max_exec_time=5)
    controller.wait()
