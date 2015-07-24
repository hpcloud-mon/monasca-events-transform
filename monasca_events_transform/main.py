import logging
import logging.config
import multiprocessing
import os
import signal
import sys
import time

from transform import Transform

from oslo_config import cfg

logging_opts = [
    cfg.StrOpt('level', default='INFO'),
    cfg.StrOpt('file', default='./transform.log'),
    cfg.StrOpt('size', default=10485760),
    cfg.StrOpt('backup', default=5),
    cfg.StrOpt('kazoo', default="WARN"),
    cfg.StrOpt('kafka', default="WARN"),
    cfg.StrOpt('statsd', default="WARN")]
logging_group = cfg.OptGroup(name='logging', title='logging')
cfg.CONF.register_group(logging_group)
cfg.CONF.register_opts(logging_opts, logging_group)

mysql_opts = [
    cfg.StrOpt('database_name'),
    cfg.StrOpt('hostname'),
    cfg.StrOpt('username'),
    cfg.StrOpt('password')]
mysql_group = cfg.OptGroup(name='mysql', title='mysql')
cfg.CONF.register_group(mysql_group)
cfg.CONF.register_opts(mysql_opts, mysql_group)

kafka_opts = [
    cfg.StrOpt('url', help='Address to kafka server. For example: '
               'url=192.168.10.4:9092'),
    cfg.StrOpt('events_topic', default='raw-events',
               help='The topic that events will be read from.'),
    cfg.StrOpt('transform_group', default='monasca-event',
               help='The group name for reading raw events.'),
    cfg.StrOpt('transform_def_topic', default='transform-definitions',
               help='The topic for transform definition events.'),
    cfg.StrOpt('transformed_events_topic', default='transformed-events',
               help='The topic for writing transformed events.')]
kafka_group = cfg.OptGroup(name='kafka', title='kafka')
cfg.CONF.register_group(kafka_group)
cfg.CONF.register_opts(kafka_opts, kafka_group)

zookeeper_opts = [
    cfg.StrOpt('url', help='Address to zookeeper server')]
zookeeper_group = cfg.OptGroup(name='zookeeper', title='zookeeper')
cfg.CONF.register_group(zookeeper_group)
cfg.CONF.register_opts(zookeeper_opts, zookeeper_group)

transform_proc_opts = [
    cfg.IntOpt(
        'number', default=1,
        help='The number of processes to start.')]
transform_proc_group = cfg.OptGroup(name='transform_processor', title='title')
cfg.CONF.register_group(transform_proc_group)
cfg.CONF.register_opts(transform_proc_opts, transform_proc_group)

cfg.CONF(sys.argv[1:],
         project='monasca',
         default_config_files=["/etc/monasca/monasca_events_transform.conf"])


log_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': "%(process)d %(asctime)s %(levelname)s %(name)s %(message)s"
        }
    },
    'handlers': {
        'console': {
            'class': "logging.StreamHandler",
            'formatter': "default"
        },
        'file': {
            'class': "logging.handlers.RotatingFileHandler",
            'filename': cfg.CONF.logging.file,
            'formatter': "default",
            'maxBytes': cfg.CONF.logging.size,
            'backupCount': cfg.CONF.logging.backup
        },
    },
    'loggers': {
        'kazoo': {'level': cfg.CONF.logging.kazoo},
        'kafka': {'level': cfg.CONF.logging.kafka},
        'statsd': {'level': cfg.CONF.logging.statsd}
    },
    'root': {
        'handlers': ['console'],
        'level': cfg.CONF.logging.level
    }
}

log = logging.getLogger(__name__)
exiting = False
processors = []


def clean_exit(signum, frame=None):
    """Exit all processes attempting to finish uncommited active work before exit.

       Can be called on an os signal or no zookeeper losing connection.
    """

    global exiting
    if exiting:
        # Since this is set up as a handler for SIGCHLD when this kills one
        # child it gets another signal, the global exiting avoids this running
        # multiple times.
        log.debug('Exit in progress clean_exit received additional signal %s' % signum)
        return

    log.info('Received signal %s, beginning graceful shutdown.' % signum)
    exiting = True

    for process in processors:
        try:
            if process.is_alive():
                process.terminate()  # Sends sigterm which any processes after a notification is sent attempt to handle
        except Exception:
            pass

    # Kill everything, that didn't already die
    for child in multiprocessing.active_children():
        log.debug('Killing pid %s' % child.pid)
        try:
            os.kill(child.pid, signal.SIGKILL)
        except Exception:
            pass

    sys.exit(signum)


def start_process():
    log.info("start transform process: {}".format(os.getpid()))
    p = Transform()
    p.run()


def main():
    logging.config.dictConfig(log_config)

    for proc in range(0, cfg.CONF.transform_processor.number):
        processors.append(multiprocessing.Process(target=start_process))

    # Start
    try:
        log.info('Starting processes')
        for process in processors:
            process.start()

        # The signal handlers must be added after the processes start otherwise
        # they run on all processes
        signal.signal(signal.SIGCHLD, clean_exit)
        signal.signal(signal.SIGINT, clean_exit)
        signal.signal(signal.SIGTERM, clean_exit)

        while True:
            time.sleep(10)

    except Exception:
        log.exception('Error! Exiting.')
        for process in processors:
            process.terminate()

if __name__ == "__main__":
    sys.exit(main())
