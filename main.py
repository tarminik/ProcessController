import multiprocessing as mp
import time


class ProcessController:
    def __init__(self):
        self.max_proc = 1
        self.task_queue = mp.Queue()
        self.manager = mp.Manager()
        self.active_tasks = self.manager.list()
        self.current_processes = []
        self.active_count = mp.Value('i', 0)
        self.lock = mp.Lock()

    def set_max_proc(self, n):
        self.max_proc = n

    def worker(self, func, args, max_exec_time, stop_event, active_tasks, active_count):
        process = mp.Process(target=self.run_with_timeout,
                             args=(func, args, max_exec_time, stop_event, active_tasks, active_count))
        process.start()
        process.join()

    def run_with_timeout(self, func, args, max_exec_time, stop_event, active_tasks, active_count):
        def target_wrapper(stop_event):
            result = func(*args)
            if stop_event.is_set():
                return
            print(f"Задание {func.__name__} с аргументами {args} завершено с результатом: {result}")

        p = mp.Process(target=target_wrapper, args=(stop_event,))
        p.start()
        p.join(max_exec_time)

        if p.is_alive():
            print(
                f"Задание {func.__name__} с аргументами {args} превысило лимит времени {max_exec_time} секунд и будет завершено.")
            stop_event.set()
            p.terminate()
            p.join()

        active_tasks.remove(func)
        with active_count.get_lock():
            active_count.value -= 1

    def start(self, tasks, max_exec_time):
        """
        :param tasks: список заданий. Задание представляет кортеж вида: (функция, кортеж входных аргументов для функции).
        :param max_exec_time: максимальное время (в секундах) работы каждого задания из списка tasks.
        :return:
        """
        for func, args in tasks:
            self.task_queue.put((func, args, max_exec_time))

        process = mp.Process(target=self._process_tasks)
        process.start()
        self.current_processes.append(process)

    def _process_tasks(self):
        local_processes = []
        while not self.task_queue.empty() or self.alive_count() > 0:
            while not self.task_queue.empty() and self.alive_count() < self.max_proc:
                func, args, max_exec_time = self.task_queue.get()
                stop_event = mp.Event()
                self.active_tasks.append(func)
                with self.active_count.get_lock():
                    self.active_count.value += 1
                p = mp.Process(target=self.worker,
                               args=(func, args, max_exec_time, stop_event, self.active_tasks, self.active_count))
                local_processes.append(p)
                p.start()
                self.current_processes.append(p)

            self._cleanup_processes(local_processes)
            time.sleep(0.1)

    def _cleanup_processes(self, local_processes):
        for p in local_processes:
            if not p.is_alive():
                local_processes.remove(p)

    def wait(self):
        while self.alive_count() > 0 or not self.task_queue.empty():
            self._cleanup_processes(self.current_processes)
            time.sleep(0.1)

    def wait_count(self):
        """
        :return: Количество заданий в очереди.
        """
        return self.task_queue.qsize()

    def alive_count(self):
        """
        :return: Количество активных процессов.
        """
        with self.active_count.get_lock():
            return self.active_count.value


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

    # Добавление новых заданий в очередь
    new_tasks = [
        (example_task, (2, "E")),
        (example_task, (2, "F"))
    ]
    controller.start(new_tasks, max_exec_time=5)

    # Демонстрация использования wait_count и alive_count
    while controller.wait_count() > 0 or controller.alive_count() > 0:
        print(f"Задания в очереди: {controller.wait_count()}")
        print(f"Активные задания: {controller.alive_count()}")
        time.sleep(1)

    controller.wait()
