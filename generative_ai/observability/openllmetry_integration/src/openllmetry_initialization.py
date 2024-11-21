# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os

from . import _gcs_image_uploader
from . import _logging_exporter

from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.exporter.cloud_monitoring import CloudMonitoringMetricsExporter
from traceloop.sdk import Traceloop
from traceloop.sdk.config import is_metrics_enabled
from traceloop.sdk.config import is_logging_enabled
from traceloop.sdk.images.image_uploader import ImageUploader


logger = logging.getLogger(__name__)


def _is_gcs_image_upload_enabled():
    """Env-driven configuration to determine whether to support uploading images to GCS.
    
    This is only relevant in the case of multi-modal agents, where you might have LLMs
    generating images that OpenLLMetry wishes to log. If this is enabled, then it is
    expected that there is also the following environment variable defined:

           GCS_IMAGE_UPLOADING_URI_PREFIX
    
    This prefix should start with "gs://your-bucket-name" and may optionally
    include additional paths like "gs://your-bucket-name/additional-prefix".

    If disabled, we prevent OpenLLMetry from attempting to upload images.
    """
    return (os.getenv('GCS_IMAGE_UPLOADING_ENABLED') or 'false').lower() in ['1', 'true']


class _NoOpImageUploader(ImageUploader):
    """A no-op implementation of the OpenLLMetry image uploader component."""

    def upload_base64_image(self, trace_id, span_id, image_name, image_file):
        logger.warn('Image uploading disabled; could not upload: {}'.format(image_name))
        return '/dev/null'


def init_openllmetry(app_name):
    """Initialize the OpenLLMetry library for use with Google Cloud.
    
    This invokes 'Traceloop.init' with the various configuration parameters
    and options intended to facilitate its use with Google Cloud.
    """
    trace_exporter = CloudTraceSpanExporter()
    metrics_exporter = None
    if is_metrics_enabled():
        metrics_exporter = CloudMonitoringMetricsExporter()
    logging_exporter = None
    if is_logging_enabled():
        _logging_exporter.CloudLoggingLogsExporter()
    image_uploader = _NoOpImageUploader()
    if _is_gcs_image_upload_enabled():
        image_uploader = _gcs_image_uploader.GcsImageUploader()
    Traceloop.init(
        app_name=app_name,
        api_endpoint='',
        exporter=trace_exporter,
        metrics_exporter=metrics_exporter,
        logging_exporter=logging_exporter,
        image_uploader=image_uploader
    )
