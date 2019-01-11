#!/bin/env python
# Osiris: Build log aggregator.

"""Observer module.

This module observer OpenShift namespace and watches for build events.
When such event occur, it puts it to [Osiris](https://github.com/CermakM/osiris)
endpoint for further processing.
"""

import os
import requests
import urllib3

import daiquiri
import logging
import typing

from http import HTTPStatus
from pathlib import Path
from requests.adapters import HTTPAdapter
from requests.adapters import Retry
from urllib.parse import urljoin

import kubernetes
from kubernetes.client.models.v1_event import V1Event as Event

from osiris.utils import noexcept
from osiris.schema.auth import Login, LoginSchema
from osiris.schema.build import BuildInfo, BuildInfoSchema

daiquiri.setup(
    level=logging.DEBUG if os.getenv('DEBUG', 0) else logging.INFO,
)

urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)


_LOGGER = daiquiri.getLogger()

_OSIRIS_HOST_NAME = os.getenv("OSIRIS_HOST_NAME", "http://0.0.0.0")
_OSIRIS_HOST_PORT = os.getenv("OSIRIS_HOST_PORT", "5000")
_OSIRIS_LOGIN_ENDPOINT = "/auth/login"
_OSIRIS_BUILD_START_HOOK = "/build/started"
_OSIRIS_BUILD_COMPLETED_HOOK = "/build/completed"

_NAMESPACE = Path('/run/secrets/kubernetes.io/serviceaccount/namespace').read_text()

_REQUESTS_MAX_RETRIES = 10


# kubernetes configuration

SERVICE_TOKEN_FILENAME = "/var/run/secrets/kubernetes.io/serviceaccount/token"
SERVICE_CERT_FILENAME = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

_KUBE_CONFIG = kubernetes.config.incluster_config.InClusterConfigLoader(
    token_filename=SERVICE_TOKEN_FILENAME, cert_filename=SERVICE_CERT_FILENAME)
_KUBE_CONFIG.load_and_set()
_KUBE_CLIENT = kubernetes.client.CoreV1Api()


class RetrySession(requests.Session):

    """RetrySession class.

    RetrySession attempts to retry failed requests and timeouts
    and holds the state between requests. Request periods are
    progressively prolonged for a certain amount of retries.
    """

    def __init__(self,
                 adapter_prefixes: typing.List[str] = None,
                 status_forcelist: typing.Tuple[int] = (500, 502, 504),
                 method_whitelist: typing.List[str] = None):

        super(RetrySession, self).__init__()

        adapter_prefixes = adapter_prefixes or ["http://", "https://"]

        retry_config = Retry(
            total=_REQUESTS_MAX_RETRIES,
            connect=_REQUESTS_MAX_RETRIES,
            backoff_factor=5,  # determines sleep time
            status_forcelist=status_forcelist,
            method_whitelist=method_whitelist
        )
        retry_adapter = HTTPAdapter(max_retries=retry_config)

        for prefix in adapter_prefixes:
            self.mount(prefix, retry_adapter)


def _authenticate(session: requests.Session):
    """Authenticate the Osiris API to the cluster with current credentials."""
    login_schema = LoginSchema()

    login = Login(
        server=os.getenv('OC_CLUSTER_SERVER'),
        token=_KUBE_CONFIG.token
    )
    login_data, _ = login_schema.dump(login)

    post_request = requests.Request(
        method='POST',
        url=urljoin(':'.join([_OSIRIS_HOST_NAME, _OSIRIS_HOST_PORT]),
                    _OSIRIS_LOGIN_ENDPOINT),
        headers={'content-type': 'application/json'},
        json=login_data
    )
    auth_request = session.prepare_request(post_request)

    login_resp = session.send(auth_request, timeout=60)

    if login_resp.status_code == HTTPStatus.ACCEPTED:

        _LOGGER.info("[AUTHENTICATION] Success.")

    else:

        _LOGGER.info("[AUTHENTICATION] Failure.")
        _LOGGER.info("[AUTHENTICATION] Status: %d  Reason: %r",
                     login_resp.status_code, login_resp.reason)

    return login_resp


@noexcept
def _is_pod_event(event: Event) -> bool:
    return event.involved_object.kind == 'Pod'


@noexcept
def _is_build_event(event: Event) -> bool:
    return event.involved_object.kind == 'Build'


@noexcept
def _is_osiris_event(event: Event) -> bool:
    # TODO: check for valid event names
    valid = _is_build_event(event) and event.reason in ['BuildStarted', 'BuildCompleted']

    return valid


if __name__ == "__main__":

    watch = kubernetes.watch.Watch()

    with RetrySession() as session:

        # authenticate osiris api
        _authenticate(session)

        put_request = requests.Request(
                url=':'.join([_OSIRIS_HOST_NAME, _OSIRIS_HOST_PORT]),
                method='PUT',
                headers={'content-type': 'application/json'}
        )

        _LOGGER.debug("Prepared request: %r", put_request)

        for streamed_event in watch.stream(_KUBE_CLIENT.list_namespaced_event,
                                           namespace=_NAMESPACE):

            kube_event: Event = streamed_event['object']

            if not _is_osiris_event(kube_event):
                continue

            _LOGGER.debug("[EVENT] New event received.")
            _LOGGER.debug("[EVENT] Event kind: %s", kube_event.kind)

            build_info = BuildInfo.from_event(kube_event)
            build_url = urljoin(_KUBE_CLIENT.api_client.configuration.host,
                                build_info.ocp_info.self_link),

            schema = BuildInfoSchema()
            data, errors = schema.dump(build_info)

            osiris_endpoint = _OSIRIS_BUILD_COMPLETED_HOOK if build_info.build_complete() else _OSIRIS_BUILD_START_HOOK

            put_request.url = urljoin(put_request.url, osiris_endpoint)
            put_request.json = data

            prep_request = session.prepare_request(put_request)

            dry_run_prefix = "[DRY-RUN] " if os.getenv("DRY_RUN", False) else ""

            _LOGGER.debug("%s[EVENT] Event to be posted: %r", dry_run_prefix, kube_event)
            _LOGGER.debug("%s[EVENT] Request: %r", dry_run_prefix, prep_request)

            _LOGGER.info("%s[EVENT] Posting event '%s' to: %s", dry_run_prefix, kube_event.kind, put_request.url)

            if not dry_run_prefix:

                resp = session.send(prep_request, timeout=60)

                if resp.status_code == HTTPStatus.ACCEPTED:

                    _LOGGER.info("[EVENT] Success.")

                else:

                    _LOGGER.info("[EVENT] Failure.")
                    _LOGGER.info("[EVENT] Status: %d  Reason: %r",
                                 resp.status_code, resp.reason)

            else:

                _LOGGER.info("%sFinished.", dry_run_prefix)
