from time import sleep

from store_beat.app import app


@app.task
def test(arg):
    print(arg)


if __name__ == '__main__':
    # celery - A store_beat.worker worker -Qworker -lINFO
    argv = ['worker', '-Astore_beat.worker', '-E', '-Qworker', '-lINFO']
    app.worker_main(argv=argv)

