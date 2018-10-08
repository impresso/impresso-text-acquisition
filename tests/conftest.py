import logging
import pkg_resources

logger = logging.getLogger()
logger.setLevel(logging.INFO)

log_file = pkg_resources.resource_filename(
    'text_importer',
    'data/tests.log'
)
handler = logging.FileHandler(filename=log_file, mode='w')
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)
