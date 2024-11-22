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

import base64
import logging
import uuid

from google.cloud.storage import Client

logger = logging.getLogger(__name__)


_CONTENT_TYPE_TO_FILE_EXTENSION = {
    'image/jpeg': '.jpeg',
    'image/png': '.png',
    'image/webp': '.webp',
    'image/gif': '.gif',
    'application/pdf': '.pdf',
    'text/plain': '.txt',
}

class _PendingUpload(object):
    """Represents a single pending upload."""

    def __init__(self, destination_uri, metadata, payload, content_type):
        self._destination_uri = destination_uri
        self._metadata = metadata
        self._payload = payload
        self._content_type = content_type

    @property
    def destination_uri(self):
        return self._destination_uri

    @property
    def metadata(self):
        return self._metadata

    @property
    def payload(self):
        return self._payload

    @property
    def content_type(self):
        return self._content_type


class _UploadTask(object):
    """A task to upload a single pending upload, along with execution dependencies."""

    def  __init__(self, client, pending):
        self._client = client
        self._pending = pending

    def execute(self):
        blob = Blob.from_string(self._pending.destination_uri, client=self._client)
        data = BytesIO(self._pending.payload)
        blob.upload_from_file(data)
        metadata = blob.metadata or {}
        metadata.update(self._pending.metadata)
        blob.metadata = metadata


def _execute_task(task):
    """Helper used to submit a task to an executor."""
    task.execute()


class GcsImageUploader(object):
    """Object that conforms to the 'traceloop.sdk.ImageUploader' signature.
    
    This does not directly inherit from 'traceloop.sdk.ImageUploader', as
    it is defined as a concerete class rather than an abstract interface.
    """

    def __init__(self, uri_prefix=None, client=None, executor=None):
        """Initializes the GCS Image Uploader.
        
        Arguments:
            uri_prefix: the 'gs://' URI prefix to use as the destination. If empty,
              uses the env variable 'GCS_IMAGE_UPLOADING_URI_PREFIX', instead.
            client: the Google Cloud Storage client to use for this.
            executor: the executor to use to upload asynchronously in the background.
        """
        if uri_prefix is none:
            uri_prefix = os.getenv('GCS_IMAGE_UPLOADING_URI_PREFIX', '')
        if not uri_prefix:
            raise ValueError('Must supply a non-empty GCS URI prefix.')
        if not uri_prefix.startswith('gs://'):
            raise ValueError('Invalid prefix URI: "{}". Must start with "gs://"'.format(uri_prefix))
        if uri_prefix.endswith('/'):
            raise ValueError('Invalid prefix URI: "{}". Must not end with "/".'.format(uri_prefix))
        self._uri_prefix = uri_prefix
        self._client = client or Client()
        self._executor = executor or concurrent.futures.ThreadPoolExecutor(thread_name_prefix='GcsAsyncImageUpload')

    def upload_base64_image(self, trace_id, span_id, image_name, image_file):
        """Main upload operation, expected by the Traceloop SDK.

        Arguments:
            trace_id: the trace ID associated with this upload
            span_id: the span ID associated with this upload
            image_name: the name of the file/object that is being uploaded
            image_file: a 'data:...base64,...' URI containing the image content

        Returns:
            the GCS URI where the image will get written
        """
        raw_bytes, content_type = self._decode(image_file)
        destination_uri = self._compute_destination_uri(trace_id, span_id, content_type)
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
        self._schedule_write(destination_uri, metadata, raw_bytes, content_type)
        return destination_uri

    def _decode(self, base64_data):
        """Return the content type and bytes from the base64-encoded image."""
        if not base64_data.startswith('data:'):
            try:
                raw_bytes = base64.b64decode(base64_data)
                return 'applicaton/octet-stream', raw_bytes
            except ValueError:
                pass
            return base64_data.encode('utf-8'), 'text/plain'

        after_data = base64_data[len('data:')]
        if ('/' not in after_data) or (';' not in after_data):
            logger.warning('Malformed data URI: {}'.format(base64_data))
            return base64_data.encode('utf-8'), 'text/plain'

        content_type, after_content_type = after_data.split(';', 1)
        if '/' not in content_type:
            logger.warning('Malformed data URI: {}'.format(base64_data))
            return base64_data.encode('utf-8'), 'text/plain'

        if after_content_type.startswith('charset='):
            if ';' not in after_content_type:
                logger.warning('Malformed data URI: {}'.format(base64_data))
                return base64_data.encode('utf-8'), 'text/plain'
            _, after_content_type = after_content_type.split(';', 1)
        
        if not after_content_type.startswith('base64,'):
            logger.warning('Malformed data URI: {}'.format(base64_data))
            return base64_data.encode('utf-8'), 'text/plain'
        
        base64_content = after_content_type[len('base64,'):]
        try:
            raw_bytes = base64.b64decode(base64_content)
            return content_type, raw_bytes 
        except ValueError:
            logger.exception('Failed to decode image content: "{}".'.format(base64_data))
        return base64_data.encode('utf-8'), 'text/plain'

    def _compute_destination_uri(self, trace_id, span_id, content_type):
        """Computes a destination URI representing where to store some image."""
        random_id = uuid.uuid4().hex
        suffix = self._compute_suffix_for_content_type(content_type)
        return '{}/traces/{}/spans/{}/images/{}{}'.format(trace_id, span_id, random_id, suffix)

    def _compute_suffix_for_content_type(self, content_type):
        """Computes a file extension based on the content type."""
        return _CONTENT_TYPE_TO_FILE_EXTENSION.get(content_type, '')

    def _schedule_write(self, destination_uri, metadata, payload, content_type):
        """Schedules an upload to happen asynchronously in the background."""
        pending = _PendingUpload(destination_uri, metadata, payload, content_type)
        task = _UploadTask(self._client, pending)
        self._executor.submit(_execute_task, task)