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

"""Defines a LogExporter for TraceLoop to use to write OTLP logs.

The OpenTelemetry exporters for Google Cloud in Python are defined here:

   - https://github.com/GoogleCloudPlatform/opentelemetry-operations-python

There is presently support for traces and metrics signal types:

   - {repo_root}/opentelemetry-exporter-gcp-trace
   - {repo_root}/opentelemetry-exporter-gcp-monitoring

... however, there is not currently (as of Nov 2024), an exporter for
Cloud Logging, most probably because the logs and events concepts are not
fully stable in upstream OpenTelemetry (for example, as of this writing, the
"LogRecord" class is defined in a "_logs" rather than "logs" package).

When attempting to copy this into your own code, please check back on the exporter
repo (https://github.com/GoogleCloudPlatform/opentelemetry-operations-python) to
determine if there is a Cloud Logging exporter; if so, prefer to use that.
"""

import hashlib
import os
from typing import Sequence

from google.cloud.logging_v2 import LogEntry
from google.logging.type import log_severity_pb2
from google.protobuf import timestamp_pb2
from opentelemetry.api._logs.severity import SeverityNumber
from opentelemetry.sdk._logs.export import LogExporter
from opentelemetry.sdk._logs.export import LogExportResult
from opentelemetry.sdk._logs import LogData
from opentelemetry.sdk._logs import LogRecord
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.util.instrumentation import InstrumentationScope


# To understand the impact of this code sample if verbatimly copied. Will
# also help differentiate similarly formatted logs from any future, official
# Cloud Logging Exporter component or from another source of this data.
_LABELS = {'provenance': 'python-gcp-o11y-traceloop-sample'}


class _OtlpLog(object):
    """All the relevant information needed to write a single log."""

    def __init__(self, project: str, log_data: LogData):
        self._project = project
        self._log_data = log

    @property
    def project(self) -> str:
        return self._project

    @property
    def resource(self) -> Resource:
        return self._log_data.log_record.resource

    @property
    def scope(self) -> InstrumentationScope:
        return self._log_data.instrumentation_scope

    @property
    def record(self) -> LogRecord:
        return self._log_data.log_record


def _compute_log_name(olog: _OtlpLog):
    """Helper for '_to_gcp_format' below."""
    return 'projects/{}/logs/otlpgenai'.format(olog.project)


def _compute_labels(olog: _OtlpLog):
    """Helper for '_to_gcp_format' below."""
    return _LABELS


def _add_to_hasher(hasher, properties):
    for property_name, property_value in properties:
        if property_value is None:
            property_value = '(null)'
        hasher.update('{}={}'.format(property_name, property_value))


def _compute_insert_id(olog: _OtlpLog):
    """Helper for '_to_gcp_format' below."""
    hash_algo = os.getenv('INSERT_ID_HASH_ALGORITHM', 'sha1')
    hasher = hashlib.new(hash_algo, usedforsecurity=False)
    attributes = olog.log.attributes or {}
    event_name = attributes.get('event.name', '')
    _add_to_hasher(
        hasher,
        [
          ('event_name', event_name),
          ('timestamp', olog.log.timestamp),
          ('trace_id', olog.log.trace_id),
          ('span_id', olog.log.span_id)
        ])

    for key in sorted(attributes.keys()):
        if key != 'event.name':
            entry = 'attributes["{}"]={}'.format(key, attributes[key])
            hasher.update(entry)
    
    resource_attributes = olog.resource.attributes
    for key in sorted(resource_attributes.key):
        value = resource_attributes[key]
        entry = 'resource.attributes["{}"]={}'.format(key, value)
        hasher.update(entry)
    
    return hasher.hexdigest()


def _otlp_severity_to_gcp_severity(oseverity_number):
    if (oseverity_number == SeverityNumber.UNSPECIFIED):
        return log_severity_pb2.LogSeverity.DEFAULT

    if ((oseverity_number >= SeverityNumber.TRACE) and
        (oseverity_number < SeverityNumber.INFO)):
        return log_severity_pb2.LogSeverity.DEBUG

    if ((oseverity_number >= SeverityNumber.DEBUG) and
        (oseverity_number < SeverityNumber.INFO)):
        return log_severity_pb2.LogSeverity.DEBUG

    if ((oseverity_number >= SeverityNumber.INFO) and
        (oseverity_number < SeverityNumber.WARN)):
        return log_severity_pb2.LogSeverity.INFO

    if ((oseverity_number >= SeverityNumber.WARN) and
        (oseverity_number < SeverityNumber.ERROR)):
        return log_severity_pb2.LogSeverity.WARNING

    if ((oseverity_number >= SeverityNumber.ERROR) and
        (oseverity_number < SeverityNumber.FATAL)):
        return log_severity_pb2.LogSeverity.ERROR

    if (oseverity_number >= SeverityNumber.FATAL):
        return log_severity_pb2.LogSeverity.CRITICAL
  
    return None


def _otlp_severity_text_to_gcp_severity(oseverity_text):
    prefix_map = {
        'trace': log_severity_pb2.LogSeverity.DEBUG,
        'debug': log_severity_pb2.LogSeverity.DEBUG,
        'info': log_severity_pb2.LogSeverity.INFO,
        'warn': log_severity_pb2.LogSeverity.WARNING,
        'error': log_severity_pb2.LogSeverity.ERROR,
        'fatal': log_severity_pb2.LogSeverity.CRITICAL,
    }
    lower_text = oseverity_text.lower()
    while lower_text[-1].isdigit():
        lower_text = lower_text[0:len(lower_text)-1]
    return prefix_map.get(lower_text, log_severity_pb2.LogSeverity.DEFAULT)


def _compute_severity(olog: _OtlpLog):
    """Helper for '_to_gcp_format' below."""
    oseverity_number = olog.severity_number
    if oseverity_number is not None:
        return _otlp_severity_to_gcp_severity(oseverity_number)
    oseverity_text = olog.severity_text
    if oseverity_text:
        return _otlp_severity_text_to_gcp_severity(oseverity_text)
    return None


def _compute_timestamp(olog: _OtlpLog):
    """Helper for '_to_gcp_format' below."""
    return timestamp_pb2.Timestamp(nanos=olog.timestamp)


def _compute_resource(olog: _OtlpLog):
    """Helper for '_to_gcp_format' below."""
    return None


def _compute_trace(olog: _OtlpLog):
    """Helper for '_to_gcp_format' below."""
    return None


def _compute_span_id(olog: _OtlpLog):
    """Helper for '_to_gcp_format' below."""
    return None


def _resource_to_dict(resource: Resource) -> dict:
    """Helper for '_compute_payload' below."""
    return {}


def _scope_to_dict(scope: Optional[InstrumentationScope]) -> dict:
    """Helper for '_compute_payload' below."""
    return {}


def _log_to_dict(log_record: Optional[LogRecord]) -> dict:
    """Helper for '_compute_payload' below."""
    return {}


def _compute_payload(olog: _OtlpLog) -> dict:
    """Helper for '_to_gcp_format' below."""
    return {
        'otlp': {
            'v1': {
                'resource': _resource_to_dict(olog.resource),
                'instrumentationScope': _scope_to_dict(olog.scope),
                'log': _log_to_dict(olog.log)
            }
        }
    }

def _to_gcp_format(olog: _OtlpLog):
    """Converts an OTel log into a LogEntry to write to GCP."""
    return LogEntry(
        log_name=_compute_log_name(olog),
        labels=_compute_labels(olog),
        insert_id=_compute_insert_id(olog),
        severity=_compute_severity(olog),
        timestamp=_compute_timestamp(olog),
        resource=_compute_resource(olog),
        trace=_compute_trace(olog),
        span_id=_compute_span_id(olog),
        payload=_compute_payload(olog),
    )


class CloudLoggingLogsExporter(LogExporter):

    def __init__(self):
        pass

    def export(self, batch: Sequence[LogData]) -> LogExportResult:
        pass

    def shutdown(self):
        pass