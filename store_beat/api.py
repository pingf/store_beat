import datetime

from aiohttp import web
from wrap.exception import async_safe

from store_beat.store import pg_store


@async_safe(Exc=Exception,
            return_value=web.json_response({'code': 500, 'where': 'api:add_job'}))
async def add_job(request):
    body = await request.json()
    if isinstance(body, dict):
        name = body.get('name')
        task = body.get('task')
        queue = body.get('queue', 'store_beat')
        # exchange = body.get('exchange', queue)
        # routing_key = body.get('routing_key', queue)
        expires = body.get('expires')
        if isinstance(expires, int):
            now = datetime.datetime.utcnow()
            expire_time = now + datetime.timedelta(seconds=expires)
            expires = expire_time.strftime("%Y-%m-%d %H:%M:%S")

        max_count = body.get('max_count')

        args = body.get('args', ())
        kwargs = body.get('kwargs', {})

        interval = body.get('interval')

        if interval:
            unit = body.get('unit', 'minutes')
            value = {
                'name': name,
                'task': task,
                'queue': queue,
                'exchange': queue,
                'routing_key': queue,
                'args': args,
                'kwargs': kwargs,
                'expires': expires,
                'max_count': max_count,

                'interval': interval,
                'unit': unit,
            }
        else:
            minute = body.get('minute', '*')
            hour = body.get('hour', '*')
            day_of_week = body.get('day_of_week', '*')
            day_of_month = body.get('day_of_month', '*')
            month_of_year = body.get('month_of_year', '*')


            value = {
                'name': name,
                'task': task,
                'queue': queue,
                'exchange': queue,
                'routing_key': queue,
                'args': args,
                'kwargs': kwargs,
                'expires': expires,
                'max_count': max_count,

                'minute': minute,
                'hour': hour,
                'day_of_week': day_of_week,
                'day_of_month': day_of_month,
                'month_of_year': month_of_year
            }

        store_job = pg_store('job')
        e_job = store_job.read(name)
        if e_job:
            store_task = pg_store('task')
            e_task = store_task.read(name)
            if e_task:
                total_run_count = e_task['value'].get('total_run_count')
                value['last_run_count'] = total_run_count
                store_job.update(name, value)
        else:
            store_job.create(name, value)

        response = web.json_response({'text': 'okay'})
        return response
    raise Exception('add job failed')


app = web.Application()
app.router.add_post('/job', add_job)

web.run_app(app, port=8082)
