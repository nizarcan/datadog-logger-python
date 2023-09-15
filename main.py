import os
from logging import StreamHandler

from datadog_api_client.v2 import ApiClient, Configuration
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.content_encoding import ContentEncoding
from datadog_api_client.v2.model.http_log import HTTPLog
from datadog_api_client.v2.model.http_log_item import HTTPLogItem
from loguru import logger

SERVICE_NAME = os.environ.get("DD_SERVICE_NAME", "python-app")


class DatadogHandler(StreamHandler):
    def __init__(self):
        super().__init__()
        configuration = Configuration()
        self.api_client = ApiClient(configuration)
        self.api_instance = LogsApi(self.api_client)

    def emit(self, record):
        log_message = self.format(record)
        log_level = record.levelname

        if record.extra:
            for key, value in record.extra.items():
                try:
                    record.extra[key] = str(value)
                except Exception:
                    record.extra.pop(key)

        log = {
            "status": log_level,
            "ddsource": "loguru",
            "ddtags": f"level:{log_level}",
            "message": log_message,
            "service": SERVICE_NAME,
            "timestamp": str(record.created),
            **record.extra,
        }

        http_log_item = HTTPLogItem(
            **log
        )

        http_log = HTTPLog([http_log_item])

        self.api_instance.submit_log(content_encoding=ContentEncoding.DEFLATE, body=http_log)


logger.add(DatadogHandler(), level="ERROR")