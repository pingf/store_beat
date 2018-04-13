from datetime import timedelta
import datetime
from celery import schedules, current_app
from celery.beat import ScheduleEntry, Scheduler
import time

from pony.orm import db_session

from store_beat.store import pg_store
from .app import app


class TaskModel:
    def __init__(self, **kwargs):
        self.name = kwargs.get('name')
        self.total_run_count = kwargs.get('total_run_count', 0)
        self.last_run_at = None


class CrontabJobModel:
    def __init__(self, **kwargs):
        self.name = kwargs.get('name')
        self.task = kwargs.get('task')
        self.queue = kwargs.get('queue')
        self.exchange = kwargs.get('exchange', self.queue)
        self.routing_key = kwargs.get('routing_key', self.queue)
        self.args = kwargs.get('args', ())
        self.kwargs = kwargs.get('kwargs', {})
        self.enabled = kwargs.get('enabled', True)

        self.last_run_count = kwargs.get('last_run_count', 0)
        self.last_run_at = None

        self.minute = kwargs.get('minute', '*')
        self.hour = kwargs.get('hour', '*')
        self.day_of_week = kwargs.get('day_of_week', '*')
        self.day_of_month = kwargs.get('day_of_month', '*')
        self.month_of_year = kwargs.get('month_of_year', '*')

        self.expires = kwargs.get('expires')
        if isinstance(self.expires, str):
            if '-' in self.expires:
                self.expires = datetime.datetime.fromtimestamp(
                    time.mktime(time.strptime(self.expires, "%Y-%m-%d %H:%M:%S")))

    @property
    def schedule(self):
        """返回 Celery schedule 对象"""
        return schedules.crontab(
            minute=self.minute,
            hour=self.hour,
            day_of_week=self.day_of_week,
            day_of_month=self.day_of_month,
            month_of_year=self.month_of_year
        )

    @staticmethod
    def compare(a, b):
        a_keys = a.__dict__.keys()
        b_keys = b.__dict__.keys()
        diff_keys = list(set(a_keys) - set(b_keys))
        if len(diff_keys) == 0:
            for k in a_keys:
                if a.__dict__.get(k) != b.__dict__.get(k):
                    return False
            return True
        return False


class PeriodJobModel:
    def __init__(self, **kwargs):
        self.name = kwargs.get('name')
        self.task = kwargs.get('task')
        self.queue = kwargs.get('queue')
        self.exchange = kwargs.get('exchange', self.queue)
        self.routing_key = kwargs.get('routing_key', self.queue)
        self.args = kwargs.get('args', ())
        self.kwargs = kwargs.get('kwargs', {})
        self.enabled = kwargs.get('enabled', True)

        self.last_run_count = kwargs.get('last_run_count', 0)
        self.last_run_at = None

        self.interval = kwargs.get('interval', 5)
        self.unit = kwargs.get('unit', 'minutes')

        self.expires = kwargs.get('expires')
        if isinstance(self.expires, str):
            if '-' in self.expires:
                self.expires = datetime.datetime.fromtimestamp(
                    time.mktime(time.strptime(self.expires, "%Y-%m-%d %H:%M:%S")))

    @property
    def schedule(self):
        return schedules.schedule(timedelta(**{self.unit: self.interval}))

    @staticmethod
    def compare(a, b):
        a_keys = a.__dict__.keys()
        b_keys = b.__dict__.keys()
        diff_keys = list(set(a_keys) - set(b_keys))
        if len(diff_keys) == 0:
            for k in a_keys:
                if a.__dict__.get(k) != b.__dict__.get(k):
                    return False
            return True
        return False


class StoreEntry(ScheduleEntry):
    store = pg_store('task')

    def __init__(self, model, app=None):
        options = dict(queue=model.queue, exchange=model.exchange,
                       routing_key=model.routing_key, expires=model.expires)
        super().__init__(
            name=model.name, task=model.task,
            last_run_at=model.last_run_at,
            total_run_count=model.last_run_count,
            schedule=model.schedule,
            args=model.args, kwargs=model.kwargs, options=options,
            app=app or current_app._get_current_object()
        )
        self.model = model

    @db_session
    def __next__(self):
        """保存任务运行信息，并返回一个新的调度器实体"""

        self.model.last_run_at = self._default_now()
        self.model.last_run_count += 1

        e = self.store.read(self.model.name)
        if e:
            self.store.update(self.model.name, {'total_run_count': self.model.last_run_count,
                                                'last_run_at': self.model.last_run_at.strftime(
                                                    "%Y-%m-%d %H:%M:%S") if self.model.last_run_at is not None else None})
        else:
            self.store.create(self.model.name, {'total_run_count': self.model.last_run_count,
                                                'last_run_at': self.model.last_run_at.strftime(
                                                    "%Y-%m-%d %H:%M:%S") if self.model.last_run_at is not None else None})

        return self.__class__(model=self.model, app=self.app)


class StoreScheduler(Scheduler):
    # UPDATE_INTERVAL = datetime.timedelta(seconds=5)

    Entry = StoreEntry
    store_job = pg_store('job')
    task_job = pg_store('task')
    flag = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 重要，定义每 5s 检查是否有任务发生变更
        self.max_interval = 5

    @property
    def schedule(self):
        elems = self.store_job.read_in('')
        tasks = []
        for e in elems:
            append_flag = True
            value = e['value']
            if not value.get('disable'):
                max_count = value.get('max_count')
                name = value.get('name')

                task = self.task_job.read(name)
                if task:
                    total_run_count = task['value'].get('total_run_count')
                    if max_count and max_count > 0 and total_run_count:
                        if total_run_count >= max_count:
                            append_flag = False
            else:
                append_flag = False

            if append_flag:
                interval = value.get('interval')
                job_model = PeriodJobModel(**value) if interval else CrontabJobModel(**value)
                tasks.append(job_model)

        # tasks = [ for e in elems if not e['value'].get('disable')]
        self._schedule = {
            task.name: self.Entry(task, app=app) for task in tasks
        }
        print(self._schedule)
        return self._schedule

    def schedules_equal(self, old_schedules, new_schedules):
        if set(old_schedules.keys()) != set(new_schedules.keys()):
            return False
        for name, old_entry in old_schedules.items():
            new_entry = new_schedules.get(name)
            if not new_entry or old_entry.schedule != new_entry.schedule or \
                    not PeriodJobModel.compare(old_entry.model, new_entry.model):
                return False
        return True

# celery - A store_beat.worker beat
