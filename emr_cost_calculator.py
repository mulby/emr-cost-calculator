"""EMR cost calculator

Usage:
    emr_cost_calculator.py total [--region=<reg> --created-after=<ca> --created-before=<cb> --aws_access_key_id=<ai> --aws_secret_access_key=<ak> --output=<format>...]
    emr_cost_calculator.py cluster --cluster-id=<ci> [--region=<reg> --aws_access_key_id=<ai> --aws_secret_access_key=<ak> --output=<format>...]
    emr_cost_calculator.py -h | --help


Options:
    -h --help                     Show this screen
    total                         Calculate the total EMR cost for a period of time
    cluster                       Calculate the cost of single cluster given the cluster id
    --region=<reg>                The aws region that the cluster was launched on [default: us-east-1]
    --aws_access_key_id=<ai>      Self-explanatory
    --aws_secret_access_key=<ci>  Self-explanatory
    --created-after=<ca>          The calculator will compute the cost for all the cluster created after the created-after day
    --created-before=<cb>         The calculator will compute the cost for all the cluster created before the created-before day
    --cluster-id=<ci>             The id of the cluster you want to calculate the cost for
    --output=<format>             The output format, valid values are: json, cloudwatch, text or datadog
    --total-start=<ts>            The calculator will compute the cost from this time until now for all clusters that were active at this time
"""

from docopt import docopt
import boto.emr
import boto.ec2
import boto.vpc
import boto.ec2.cloudwatch
from retrying import retry
import sys
import time
import math
import yaml
import datetime
from operator import attrgetter
import json
import datadog
import datadog.api
import os


config = yaml.load(open('config.yml', 'r'))
prices = config['prices']
emr_prices = config['emr']


def validate_date(date_text):
    try:
        return datetime.datetime.strptime(date_text, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
       raise ValueError('Incorrect data format, should be YYYY-MM-DDThh:mm:ssZ')


def retry_if_EmrResponseError(exception):
    """
    Use this function in order to back off only
    on EmrResponse errors and not in other exceptions
    """
    if isinstance(exception, boto.exception.EmrResponseError):
        print >> sys.stderr, '[WARN] EmrResponseError detected, backing off before retrying.'
        return True
    return False


class Ec2Instance:

    def __init__(self, creation_ts, termination_ts, pricing, overhead, total_start):
        self.creation_time = creation_ts
        creation_ts = Ec2Instance._parse_date(creation_ts)
        self.termination_time = termination_ts
        termination_ts = Ec2Instance._parse_date(termination_ts)
        self.total_start_time = total_start

        if total_start >= termination_ts:
            self.cost = 0
            self.lifetime = 0
            return

        if creation_ts <= total_start < termination_ts:
            creation_ts = total_start

        lifetime = math.ceil((termination_ts - creation_ts).total_seconds() / 3600.0)
        self.lifetime = lifetime
        emr_cost = lifetime * overhead
        if isinstance(pricing, float):
            ec2_cost = lifetime * pricing
        else:
            sorted_pricing = sorted(pricing, key=attrgetter('timestamp'))
            ec2_cost = None
            current_time = creation_ts
            idx = 0
            while current_time < termination_ts and idx < len(sorted_pricing):
                pricing = sorted_pricing[idx]
                current_time = Ec2Instance._parse_date(pricing.timestamp)
                if current_time >= creation_ts:
                    if (idx + 1) < len(sorted_pricing):
                        next_timestamp = Ec2Instance._parse_date(sorted_pricing[idx + 1].timestamp)
                    else:
                        next_timestamp = termination_ts
                        current_time = termination_ts

                    if ec2_cost == None:
                        time_diff = next_timestamp - creation_ts
                        ec2_cost = 0
                    else:
                        time_diff = next_timestamp - current_time

                    ec2_cost += (time_diff.total_seconds() / 3600.0) * pricing.price
                idx += 1

            if ec2_cost is None:
                ec2_cost = lifetime * pricing.price

        self.cost = ec2_cost + emr_cost

    @staticmethod
    def _parse_date(timestamp):
        return datetime.datetime.strptime(timestamp.rstrip('Z').rsplit('.', 1)[0], '%Y-%m-%dT%H:%M:%S')


class InstanceGroup:

    def __init__(self, group):
        self.group_id = group.id
        self.instance_type = group.instancetype
        self.group_type = group.instancegrouptype
        self.market = group.market
        self.overhead = emr_prices[self.instance_type]


class EmrCostCalculator:

    def __init__(self, region, aws_access_key_id=None, aws_secret_access_key=None):
        try:
            print >> sys.stderr, \
                '[INFO] Retrieving cost in region %s' \
                % (region)
            self.conn = \
                boto.emr.connect_to_region(
                    region,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)

            self.ec2_conn = boto.ec2.connect_to_region(
                    region,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)

            self.cw_conn = boto.ec2.cloudwatch.connect_to_region(
                    region,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)

            self.vpc_conn = boto.vpc.connect_to_region(
                    region,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)
        except:
            print >> sys.stderr, \
                '[ERROR] Could not establish connection with EMR api'

    def get_total_cost_by_dates(self, created_after, created_before, total_start):
        print >> sys.stderr, '[INFO] Finding clusters created between {after} and {before}.'.format(
            after=created_after.isoformat(),
            before=created_before.isoformat()
        )
        total_cost = 0
        cluster_costs = []
        for cluster in self._get_cluster_list(created_after, created_before, total_start):
            cost_dict = self.get_cluster_cost(cluster, total_start)
            cluster_costs.append(cost_dict)
            total_cost += cost_dict['TOTAL']
        return cluster_costs

    @retry(wait_exponential_multiplier=500,
           wait_exponential_max=7000,
           retry_on_exception=retry_if_EmrResponseError)
    def get_cluster_cost(self, cluster, total_start):
        """
        Joins the information from the instance groups and the instances
        in order to calculate the price of the whole cluster

        It is important that we use a backoff policy in this case since Amazon
        throttles the number of API requests.
        :return: A dictionary with the total cost of the cluster and the
                individual cost of each instance group (Master, Core, Task)
        """
        instance_groups = self._get_instance_groups(cluster)
        cost_dict = {
            'id': cluster.id,
            'name': cluster.name
        }
        print >> sys.stderr, '[DEBUG] Analyzing cluster {cluster_id} named {name}.'.format(
            cluster_id=cluster.id,
            name=cluster.name
        )
        for instance_group in instance_groups:
            print >> sys.stderr, '[DEBUG] Analyzing instance group {ig}'.format(
                ig=instance_group.group_type
            )
            first = True
            for instance in self._get_instances(instance_group, cluster, total_start):
                cost_dict.setdefault(instance_group.group_type, 0)
                cost_dict[instance_group.group_type] += instance.cost
                cost_dict.setdefault('TOTAL', 0)
                cost_dict['TOTAL'] += instance.cost
                cost_dict['creation_time'] = instance.creation_time
                cost_dict['termination_time'] = instance.termination_time
                cost_dict['lifetime'] = instance.lifetime
                cost_dict['date'] = instance.creation_time.split('T')[0]

                if first:
                    print >> sys.stderr, '[DEBUG] Instance cost ${0} for {1} hours of computation.'.format(
                        instance.cost, instance.lifetime
                    )
                    first = False

        return EmrCostCalculator._sanitise_floats(cost_dict)

    @staticmethod
    def _sanitise_floats(aDict):
        """
        Round the values to 3 decimals.
        #Did it this way to avoid
        https://docs.python.org/2/tutorial/floatingpoint.html#representation-error
        """
        for key in aDict:
            if key in ('TOTAL', 'MASTER', 'CORE', 'SPOT'):
                aDict[key] = round(aDict[key], 3)
        return aDict

    def _get_cluster_list(self, created_after, created_before, total_start):
        """
        :return: An iterator of cluster ids for the specified dates
        """
        marker = None
        while True:
            cluster_list = \
                self._get_raw_cluster_list(created_after,
                                           created_before,
                                           marker=marker)
            for cluster in cluster_list.clusters:
                cluster_end_time = getattr(cluster.status.timeline, 'enddatetime', None)
                if cluster_end_time:
                    end_time_ts = Ec2Instance._parse_date(cluster_end_time)
                    if end_time_ts <= total_start:
                        continue
                yield self.conn.describe_cluster(cluster.id)
            try:
                marker = cluster_list.marker
            except AttributeError:
                break

    @retry(wait_exponential_multiplier=500,
           wait_exponential_max=7000,
           retry_on_exception=retry_if_EmrResponseError)
    def _get_raw_cluster_list(self, created_after, created_before, marker):
        return self.conn.list_clusters(created_after,
                                       created_before,
                                       marker=marker)

    def _get_instance_groups(self, cluster):
        """
        Invokes the EMR api and gets a list of the cluster's instance groups.
        :return: List of our custom InstanceGroup objects
        """
        groups = self.conn.list_instance_groups(cluster.id).instancegroups
        instance_groups = []
        for group in groups:
            instance_groups.append(InstanceGroup(group))
        return instance_groups

    def _get_instances(self, instance_group, cluster, total_start):
        """
        Invokes the EMR api to retrieve a list of all the instances
        that were used in the cluster.
        This list is then joind to the InstanceGroup list
        on the instance group id
        :return: An iterator of our custom Ec2Instance objects.
        """
        instance_list = self.conn.list_instances(cluster.id, instance_group.group_id).instances
        pricing = None
        for instance_info in instance_list:
            if pricing is None:
                timeline = instance_info.status.timeline
                start_time = timeline.creationdatetime
                if hasattr(timeline, 'enddatetime'):
                    end_time = timeline.enddatetime
                else:
                    end_time = datetime.datetime.utcnow().isoformat()

                if instance_group.market == 'SPOT':
                    try:
                        availability_zone = cluster.ec2instanceattributes.ec2availabilityzone
                    except AttributeError:
                        subnets = self.vpc_conn.get_all_subnets(subnet_ids=[cluster.ec2instanceattributes.ec2subnetid])
                        availability_zone = subnets[0].availability_zone

                    print >> sys.stderr, \
                        '[DEBUG] Gathering spot market pricing data for {instance_type} from {from_date} to {to_date} in {az}.'.format(
                            instance_type=instance_group.instance_type,
                            from_date=start_time,
                            to_date=end_time,
                            az=availability_zone
                        )
                    pricing = self.ec2_conn.get_spot_price_history(
                        instance_type=instance_group.instance_type,
                        start_time=start_time,
                        end_time=end_time,
                        availability_zone=availability_zone
                    )
                    print >> sys.stderr, '[DEBUG] Spot market data downloaded.'
                else:
                    pricing = prices[instance_group.instance_type]

            try:
                inst = Ec2Instance(
                            start_time,
                            end_time,
                            pricing,
                            instance_group.overhead,
                            total_start)
                yield inst
            except AttributeError:
                print >> sys.stderr, \
                    '[WARN] Error when computing instance cost. Cluster: %s'\
                    % cluster.id

    def output_cluster_costs(self, costs, output_format):
        if output_format == 'json':
            print json.dumps(costs)
        elif output_format == 'cloudwatch':
            for cluster_costs in costs:
                self.cw_conn.put_metric_data(
                    namespace='Analytics/Monitor',
                    name='EmrClusterCost',
                    value=cluster_costs['TOTAL'],
                    timestamp=Ec2Instance._parse_date(cluster_costs['termination_time']),
                    unit='Count',
                    dimensions={
                        "JobFlowName": cluster_costs['name'],
                    },
                )
        elif output_format == 'datadog':
            datadog.initialize(
                api_key=os.environ['DATADOG_API_KEY'],
                app_key=os.environ['DATADOG_APP_KEY']
            )
            for cluster_costs in costs:
                python_timestamp = Ec2Instance._parse_date(cluster_costs['termination_time'])
                posix_timestamp = time.mktime(python_timestamp.timetuple())
                datadog.api.Metric.send(
                    metric='edx.analytics.emr.cost',
                    points=(posix_timestamp, cluster_costs['TOTAL']),
                    tags=['jobflowname' + cluster_costs['name']]
                )
        elif output_format == 'text':
            for cluster_costs in costs:
                print >> sys.stderr, '[DEBUG] Cluster {name} cost ${cost} on {timestamp}'.format(
                    name=cluster_costs['name'],
                    cost=cluster_costs['TOTAL'],
                    timestamp=cluster_costs['termination_time']
                )
            total_cost = 0
            for cost in clusters:
                total_cost += cost['TOTAL']
            print total_cost
        else:
            raise RuntimeError('Invalid output format: {}'.format(output_format))


if __name__ == '__main__':
    args = docopt(__doc__)
    calc = EmrCostCalculator(args['--region'],
                             args.get('--aws_access_key_id'),
                             args.get('--aws_secret_access_key'))

    if args.get('total'):
        current_time = datetime.datetime.utcnow()
        one_day_ago = current_time - datetime.timedelta(hours=23)

        created_after_str = args.get('--created-after')
        if not created_after_str:
            created_after = current_time - datetime.timedelta(days=365)
        else:
            created_after = validate_date(created_after_str)

        created_before_str = args.get('--created-before')
        if not created_before_str:
            created_before = current_time
        else:
            created_before = validate_date(created_before_str)

        total_start_str = args.get('--total-start')
        if not total_start_str:
            total_start = one_day_ago
        else:
            total_start = validate_date(total_start_str)

        clusters = calc.get_total_cost_by_dates(created_after, created_before, total_start)
    elif args.get('cluster'):
        cluster = calc.conn.describe_cluster(args.get('--cluster-id'))
        clusters = [calc.get_cluster_cost(cluster)]
    else:
        print >> sys.stderr, '[ERROR] Invalid operation, please check usage again'
        sys.exit(1)

    for output_format in args['--output']:
        calc.output_cluster_costs(clusters, output_format)

    total_cost = 0
    for cost in clusters:
        total_cost += cost['TOTAL']

    print >> sys.stderr, '[INFO] Total Cost: ${}'.format(total_cost)
