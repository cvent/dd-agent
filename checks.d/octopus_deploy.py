# stdlib
import itertools
import requests
requests.packages.urllib3.disable_warnings()
import re

# project
from checks import AgentCheck

class OctopusDeploy(AgentCheck):

    prefix = 'octopus_deploy'

    def __init__(self, name, init_config, agentConfig):
        AgentCheck.__init__(self, name, init_config, agentConfig)
        self.high_watermarks = {}

    def _get_octo(self, path):
        path = re.sub(r'^/?api/', '', path)
        request_headers = {'content-type': 'application/json', 'X-Octopus-ApiKey': self.api_key }
        url = self.endpoint + '/api/' + path
        return requests.get(url, verify=False, timeout=2, headers=request_headers).json()

    def _get_recent_octo(self, generator, watermarkId):
        latest_id = self.high_watermarks.get(watermarkId)
        if not latest_id:
            self.high_watermarks[watermarkId] = next(generator)['Id']
            return []
        items = list(itertools.takewhile(lambda x: not x.get('Id') or x.get('Id') != latest_id, generator))

        self.log.info("Items: %s" % items)

        if items:
            self.high_watermarks[watermarkId] = items[0]['Id']
        return items

    def _get_octo_generator(self, path, recurse = True):
        response = self._get_octo(path)
        if 'ItemsPerPage' in response:
            if recurse:
                try:
                    for page in (response['Links']["Page.%s" % i] for i in itertools.count()):
                        for item in self._get_octo_generator(page, recurse = False):
                            yield item
                except KeyError:
                    raise StopIteration
            else:
                for item in response['Items']:
                    yield item
        else:
            yield response

    def _get_completed_deployment_tasks(self):
        inactive_tasks = self._get_octo_generator('tasks?active=false')
        return itertools.ifilter(lambda x: x['Name'] == 'Deploy', inactive_tasks)

    def _get_recent_completed_deployment_tasks(self):
        return self._get_recent_octo(self._get_completed_deployment_tasks(), 'deploy_tasks')

    def check(self, instance):
        self.endpoint = instance.get('endpoint')
        self.api_key = instance.get('api-key')

        if not self.endpoint:
            raise Exception('No endpoint found in the config file.')
        if not self.api_key:
            raise Exception('No api-key found in the config file.')

        # Check maintenance mode
        maintenance = self._get_octo('serverstatus')['IsInMaintenanceMode']
        self.service_check("%s.maintenance_mode" % self.prefix, 1 if maintenance else 0)

        # Count the active tasks
        active_tasks = self._get_octo('tasks?active=true')['TotalResults']
        self.count("%s.tasks.active.count" % self.prefix, active_tasks)

        # Count the queued deployments
        server_activities = [service for service in self._get_octo('serverstatus/activities')['SystemServices'] if service['ActorName'] == 'Octopus.Logger']
        for service in server_activities:
            self.count("%s.logs.queued.count" % self.prefix, service['InputQueueCount'])

        # Count the environments, projects and machines
        self.count("%s.environments.count" % self.prefix, self._get_octo('environments')['TotalResults'])
        self.count("%s.projects.count" % self.prefix, self._get_octo('projects')['TotalResults'])
        self.count("%s.machines.count" % self.prefix, self._get_octo('machines')['TotalResults'])

        # Count the deployment success/failures
        for item in self._get_recent_completed_deployment_tasks():
            deploy = self._get_octo("deployments/%s" % item['Arguments']['DeploymentId'])

            tags = [
                "octo_environment:%s" % self._get_octo(deploy['Links']['Environment'])['Name'],
                "octo_project:%s" % self._get_octo(deploy['Links']['Project'])['Name']
            ]

            if item['FinishedSuccessfully']:
                self.increment("%s.deploy.success" % self.prefix, tags=tags)
            else:
                self.increment("%s.deploy.failure" % self.prefix, tags=tags)
