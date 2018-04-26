import datetime
import uuid

from aiohttp import web
import os

from aiohttp.web_urldispatcher import StaticDef
from wrap.exception import async_safe

from store_beat.store import pg_store


def gen_uuid():
    return str(uuid.uuid1().hex)


@async_safe(Exc=Exception,
            return_value=web.json_response({'code': 500, 'where': 'api:add_job'}))
async def add_job(request):
    body = await request.json()
    if isinstance(body, dict):
        name = body.get('name')
        task = body.get('task')
        jid = body.get('jid') or gen_uuid()
        queue = body.get('queue', 'store_beat')
        # exchange = body.get('exchange', queue)
        # routing_key = body.get('routing_key', queue)
        expires = body.get('expires')
        if isinstance(expires, int):
            now = datetime.datetime.utcnow()
            expire_time = now + datetime.timedelta(seconds=expires)
            expires = expire_time.strftime("%Y-%m-%d %H:%M")

        max_count = body.get('max_count')
        if isinstance(max_count, str):
            max_count = int()

        args = body.get('args', ())
        kwargs = body.get('kwargs', {})

        interval = body.get('interval')
        disabled = body.get('disabled')

        if interval:
            unit = body.get('unit', 'minutes')
            value = {
                'name': name,
                'jid': jid,
                'task': task,
                'queue': queue,
                'exchange': queue,
                'routing_key': queue,
                'args': args,
                'kwargs': kwargs,
                'expires': expires,
                'max_count': max_count if max_count and max_count > 0 else None,
                'interval': interval,
                'unit': unit,
                'disabled': disabled
            }
        else:
            minute = body.get('minute', '*')
            hour = body.get('hour', '*')
            day_of_week = body.get('day_of_week', '*')
            day_of_month = body.get('day_of_month', '*')
            month_of_year = body.get('month_of_year', '*')

            value = {
                'name': name,
                'jid': jid,
                'task': task,
                'queue': queue,
                'exchange': queue,
                'routing_key': queue,
                'args': args,
                'kwargs': kwargs,
                'expires': expires,
                'max_count': max_count if max_count and max_count > 0 else None,

                'minute': minute,
                'hour': hour,
                'day_of_week': day_of_week,
                'day_of_month': day_of_month,
                'month_of_year': month_of_year,
                'disabled': disabled
            }

        store_job = pg_store('job')
        e_job = store_job.read(jid)
        if e_job:
            store_job.update(jid, value)
        else:
            store_job.create(jid, value)

        response = web.json_response({'text': 'okay'})
        return response
    raise Exception('add job failed')


@async_safe(Exc=Exception,
            return_value=web.json_response({'code': 500, 'where': 'api:add_job'}))
async def list_job(request):
    store_job = pg_store('job')
    store_task = pg_store('task')
    jobs = store_job.read_in('')
    # tasks = store_task.read_in('')
    results = []
    for job in jobs:
        elem = job['value']
        jid = elem.get('jid')
        task = store_task.read(jid)
        if task:
            elem.update(task['value'])
        elem.update({'create_at': job['create_at']})
        results.append(elem)
    # for task in tasks:
    #     print(task)
    response = web.json_response({'text': 'okay', 'data': results})
    return response


@async_safe(Exc=Exception,
            return_value=web.json_response({'code': 500, 'where': 'api:add_job'}))
async def update_job(request):
    body = await request.json()
    if isinstance(body, dict):
        jid = body.get('jid')

        store_job = pg_store('job')

        store_job.update(jid, body)
        response = web.json_response({'text': 'okay'})
        return response
    raise Exception('update job failed!')

@async_safe(Exc=Exception,
            return_value=web.json_response({'code': 500, 'where': 'api:delete_job'}))
async def delete_job(request):
    body = await request.json()
    if isinstance(body, list):
        jids = body

        store_job = pg_store('job')
        for k in jids:
            store_job.delete(k)
        response = web.json_response({'text': 'okay'})
        return response
    raise Exception('delete job failed!')




__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

INDEX = open(os.path.join(__location__, '../front/dist/index.html')).read().encode('utf-8')


async def handle_index(request):
    # return web.HTTPFound('/index.html')
    return web.Response(body=INDEX, content_type='text/html')


route = web.StaticDef(path=__location__+'/../front/dist/', prefix='/', kwargs={})


if __name__ == '__main__':
    app = web.Application()
    app.router.add_post('/job', add_job)
    app.router.add_get('/job', list_job)
    app.router.add_put('/job', update_job)
    app.router.add_delete('/job', delete_job)

    app.router.add_get('/', handle_index)
    # app.router.add_routes(route)
    route.register(app.router)

    # app.router.st


    # app.router.add_get('/js_clusterize', handle_js)
    # app.router.add_get('/css_clusterize', handle_css)
    web.run_app(app, port=8082)
