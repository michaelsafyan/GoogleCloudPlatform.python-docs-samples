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

"""Defines an Traceloop ImageUploader that uses Google Cloud Storage as a backend."""

import logging
import uuid

from traceloop.sdk.images.image_uploader import ImageUploader


logger = logging.getLogger(__name__)


class GcsImageUploader(object):
    """An instance of the Traceloop SDK 'ImageUploader' that writes data to GCS."""

    def __init__(self, uri_prefix=None, executor=None):
        if uri_prefix is none:
            uri_prefix = os.getenv('GCS_IMAGE_UPLOADING_URI_PREFIX', '')
        if not uri_prefix:
            raise ValueError('Must supply a non-empty GCS URI prefix.')
        if not uri_prefix.startswith('gs://'):
            raise ValueError('Invalid prefix URI: "{}". Must start with "gs://"'.format(uri_prefix))
        if uri_prefix.endswith('/'):
            raise ValueError('Invalid prefix URI: "{}". Must not end with "/".'.format(uri_prefix))
        self._uri_prefix = uri_prefix
        self._executor = executor or concurrent.futures.ThreadPoolExecutor()

    def upload_base64_image(self, trace_id, span_id, image_name, image_file):
        destination_uri = self._compute_destination_uri(trace_id, span_id)
        metadata = {
            # For linking back to the original trace.
            'trace_id': trace_id,
            'span_id': span_id,

            # For understanding where the image came from.
            'original_image_name': image_name,

            # For understanding how much of this usage of GCP comes from
            # verbatim reuse of this specific code sample.
            'provenance': 'python-gcp-o11y-traceloop-sample',
        }
        self._schedule_write(destination_uri, metadata, image_file)
        return destination_uri
    
    def _compute_destination_uri(self, trace_id, span_id):
        random_id = uuid.uuid4().hex
        return '{}/traces/{}/spans/{}/images/{}'.format(trace_id, span_id, random_id)

    def _schedule_write(self, destination_uri, metadata, payload):
        logger.info('Not writing "{}"; not yet implemented.'.format(destination_uri))
