from datetime import timedelta
import datetime
from celery import schedules, current_app
from celery.beat import ScheduleEntry, Scheduler
import time

from pony.orm import db_session

from store_beat.api import gen_uuid
from store_beat.store import pg_store
from .app import app


# class TaskModel:
#     def __init__(self, **kwargs):
#         self.jid = kwargs.get('jid')
#         self.total_run_count = kwargs.get('total_run_count', 0)
#         self.last_run_at = None
# def get_last_run_count(jid):
#     store_task = pg_store('task')
#     e = store_task.read(jid)
#     if e:
#         return e.get('total_run_count') or 0
#     return 0
#
#
# def get_last_run_at(jid):
#     store_task = pg_store('task')
#     e = store_task.read(jid)
#     if e:
#         return e.get('last_run_at') or None
#     return None


class CrontabJobModel:
    def __init__(self, **kwargs):
        self.jid = kwargs.get('jid')
        self.name = kwargs.get('name')
        self.task = kwargs.get('task')
        self.queue = kwargs.get('queue')
        self.exchange = kwargs.get('exchange', self.queue)
        self.routing_key = kwargs.get('routing_key', self.queue)
        self.args = kwargs.get('args', ())
        self.kwargs = kwargs.get('kwargs', {})
        self.enabled = kwargs.get('enabled', True)

        # self.last_run_count = get_last_run_count(self.jid)
        # self.last_run_at = get_last_run_at(self.jid)

        self.minute = kwargs.get('minute', '*')
        self.hour = kwargs.get('hour', '*')
        self.day_of_week = kwargs.get('day_of_week', '*')
        self.day_of_month = kwargs.get('day_of_month', '*')
        self.month_of_year = kwargs.get('month_of_year', '*')

        self.expires = kwargs.get('expires')
        if isinstance(self.expires, str):
            if '-' in self.expires:
                self.expires = datetime.datetime.fromtimestamp(
                    time.mktime(time.strptime(self.expires, "%Y-%m-%d %H:%M")))

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
        self.jid = kwargs.get('jid')
        self.name = kwargs.get('name')
        self.task = kwargs.get('task')
        self.queue = kwargs.get('queue')
        self.exchange = kwargs.get('exchange', self.queue)
        self.routing_key = kwargs.get('routing_key', self.queue)
        self.args = kwargs.get('args', ())
        self.kwargs = kwargs.get('kwargs', {})
        self.enabled = kwargs.get('enabled', True)

        # self.last_run_count = get_last_run_count(self.jid)
        # self.last_run_at = get_last_run_at(self.jid)

        self.interval = kwargs.get('interval', 5)
        self.unit = kwargs.get('unit', 'minutes')

        self.expires = kwargs.get('expires')
        if isinstance(self.expires, str):
            if '-' in self.expires:
                self.expires = datetime.datetime.fromtimestamp(
                    time.mktime(time.strptime(self.expires, "%Y-%m-%d %H:%M")))

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
    store_task = pg_store('task')
    store_tasks = pg_store('tasks')

    def __init__(self, model, app=None):
        options = dict(queue=model.queue, exchange=model.exchange,
                       routing_key=model.routing_key, expires=model.expires)
        super().__init__(
            name=model.jid, task=model.task,
            # last_run_at=model.last_run_at,
            # total_run_count=model.last_run_count,
            schedule=model.schedule,
            args=model.args, kwargs=model.kwargs, options=options,
            app=app or current_app._get_current_object()
        )
        self.model = model

    @db_session
    def __next__(self):
        """保存任务运行信息，并返回一个新的调度器实体"""

        now = self._default_now().strftime("%Y-%m-%d %H:%M:%S")

        e = self.store_task.read(self.model.jid)
        if e:
            v = e['value']
            total_run_count = v['total_run_count'] + 1
            last_run_at = now
            self.store_task.update(self.model.jid, {
                'total_run_count': total_run_count,
                'last_run_at': last_run_at
            })
        else:
            total_run_count = 1
            last_run_at = now
            self.store_task.create(self.model.jid, {
                'jid': self.model.jid,
                'total_run_count': total_run_count,
                'last_run_at': last_run_at
            })

        tid = gen_uuid()
        tasks_data = {
            'tid': tid,
            'jid': self.model.jid,
            'name': self.model.name,
            'count': total_run_count,
            'kwargs': self.model.kwargs,
            'args': self.model.args,
            'queue': self.model.queue,
            'at': last_run_at
        }


        if isinstance(self.model, PeriodJobModel):
            tasks_data['interval'] = self.model.interval
            tasks_data['unit'] = self.model.unit
        if isinstance(self.model, CrontabJobModel):
            tasks_data['minute'] = self.model.minute
            tasks_data['hour'] = self.model.hour
            tasks_data['day_of_week'] = self.model.day_of_week
            tasks_data['day_of_month'] = self.model.day_of_month
            tasks_data['month_of_year'] = self.model.month_of_year

        results = self.store_tasks.queryd({'jid': self.model.jid})
        self.store_tasks.create(tid, tasks_data)
        if len(results) > 120:
            to_deletes = results[100:]
            for e in to_deletes:
                key = e['key']
                self.store_tasks.delete(key=key)
        return self.__class__(model=self.model, app=self.app)


class StoreScheduler(Scheduler):
    # UPDATE_INTERVAL = datetime.timedelta(seconds=5)

    Entry = StoreEntry
    store_job = pg_store('job')
    store_task = pg_store('task')
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
            jid = value.get('jid')
            if not value.get('disabled'):
                max_count = value.get('max_count')

                task = self.store_task.read(jid)
                if task:

                    total_run_count = task['value'].get('total_run_count')
                    if max_count:
                        if isinstance(max_count, str):
                            max_count = int(max_count)
                        if max_count > 0 and total_run_count:
                            if total_run_count >= max_count:
                                append_flag = False
            else:
                append_flag = False

            if append_flag:
                interval = value.get('interval')
                job_model = PeriodJobModel(**value) if interval else CrontabJobModel(**value)
                tasks.append(job_model)

        self._schedule = {
            task.jid: self.Entry(task, app=app) for task in tasks
        }
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
