"""Build log processing."""

import re
import boto3
from botocore.client import Config
from urllib.parse import quote_plus

import config
import lambdalogging

LOG = lambdalogging.getLogger(__name__)

CODEBUILD = boto3.client('codebuild')
CW_LOGS = boto3.client('logs')
BUCKET = boto3.resource('s3', config=Config(signature_version='s3v4')).Bucket(config.BUCKET_NAME)


class Build:
    """Encapsulate logic around CodeBuild builds and copying logs."""

    def __init__(self, build_event):
        """Create new Build helper object."""
        self._build_event = build_event
        self.id = build_event['detail']['build-id']
        self.project_name = build_event['detail']['project-name']
        self.status = build_event['detail']['build-status']
        self.group_identifier = None

    def get_pr_id(self):
        """If this build was for a PR branch, returns the PR ID, otherwise returns None."""
        matches = re.match(r'^pr\/(\d+)', self._get_build_details().get('sourceVersion', ""))
        if not matches:
            return None
        return int(matches.group(1))

    @property
    def commit_id(self):
        """Return the commit ID for this build."""
        return self._get_build_details()["resolvedSourceVersion"]

    def is_pr_build(self):
        """Return True if this build is associated with a PR."""
        return self.get_pr_id() is not None

    def copy_logs(self):
        """Copy build logs to app S3 bucket and return a URL."""
        log_info = self._get_build_details()['logs']
        log_group = log_info['groupName']
        log_stream = log_info['streamName']
        paginator = CW_LOGS.get_paginator('filter_log_events')

        iter = paginator.paginate(
            logGroupName=log_group,
            logStreamNames=[log_stream]
        )
        logs_content = ''.join([event['message'] for page in iter for event in page['events']])

        BUCKET.put_object(
            Key=self._get_logs_key(),
            Body=logs_content,
            ContentType='text/plain'
        )

    def get_logs_url(self):
        """Return URL to build logs."""
        return '{}?key={}'.format(config.BUILD_LOGS_API_ENDPOINT, quote_plus(self._get_logs_key()))

    def _get_logs_key(self):
        log_stream = self._get_build_details()['logs']['streamName']
        return '{}/build.log'.format(log_stream)

    def _get_build_details(self):
        if not hasattr(self, '_build_details'):
            response = CODEBUILD.batch_get_builds(ids=[self.id])
            self._build_details = response['builds'][0]
            self._process_batch()
            LOG.debug('Build %s details: %s', self.id, self._build_details)
        return self._build_details

    def _process_batch(self):
        if 'buildBatchArn' not in self._build_details:
            return

        # Get batch from batch item details received in the event
        try:
            batch = CODEBUILD.batch_get_build_batches(
                ids=[self._build_details['buildBatchArn'].split('/')[1]]
            )['buildBatches'][0]
        except KeyError:
            return

        if 'sourceVersion' not in batch:
            return

        # Batch contains the `pr/123` as sourceVersion if the event was fired for a PR
        # Pushing it up to the `_build_details` object which has the commit ID as sourceVersion
        # Without overwriting sourceVersion makes `is_pr_build` always return False
        # Resulting in no logs transferred for batch builds
        self._build_details['sourceVersion'] = batch['sourceVersion']

        if 'buildGroups' not in batch:
            return

        # Finding "group" (item of the batch for which the event was fired)
        def iterator_callback(group):
            if 'currentBuildSummary' not in group:
                return False

            if 'arn' not in group['currentBuildSummary']:
                return False

            return group['currentBuildSummary']['arn'] == self._build_details['arn']

        iterator = filter(iterator_callback, batch['buildGroups'])
        group = next(iterator, False)

        # If group not found, return early
        if group is False:
            return

        if 'identifier' not in group:
            return

        # group_identifier is the name of the item in the batch
        # Saving it to a property for further processing later
        self.group_identifier = group['identifier']

        # Setting build status to group item status to correctly report build status
        # Without this the status of the item would inherit the status of the batch
        # which is undesired behaviour for a successful batch item in a batch where other items are failing
        self.status = group['currentBuildSummary']['buildStatus']
