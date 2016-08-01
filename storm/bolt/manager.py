#!/usr/bin/env python
"""
This utility provides functionality to provision and configure clusters

"""
from __future__ import print_function

import ConfigParser
import argparse
import logging
import os
import sys
import time

from libcloud.compute.base import NodeDriver
from libcloud.compute.providers import get_driver
from prettytable import PrettyTable

from c4.utils.util import getModuleClasses
from storm.thunder.base import NodesInfoMap


log = logging.getLogger(__name__)

DEFAULT_CPUS = 2
DEFAULT_IMAGE_ID = "centos-7.2"
DEFAULT_NUMBER_OF_NODES = 3
DEFAULT_RAM = 2048

class Bolt(object):
    """
    A cloud manager implementation that utilizes the specified driver
    to create, manage and destroy clusters and nodes.

    :param driver: driver
    :type driver: :class:`~libcloud.compute.base.NodeDriver`
    """
    def __init__(self, driver):
        self.driver = driver

    def createCluster(
            self,
            cluster=None,
            cpus=DEFAULT_CPUS,
            disks=None,
            imageId=DEFAULT_IMAGE_ID,
            nodes=None,
            numberOfNodes=DEFAULT_NUMBER_OF_NODES,
            ram=DEFAULT_RAM
        ):
        """
        Create a new cluster. All nodes in this cluster will be started
        automatically.

        :param cluster: cluster name
        :type cluster: str
        :param cpus: number of cpus per node
        :type cpus: int
        :param disks: list of disks capacities in GB
        :type disks: list
        :param imageId: id of the image to use for the nodes
        :type imageId: str
        :param nodes: list of node names
        :type nodes: []
        :param numberOfNodes: number of nodes
        :type numberOfNodes: int
        :param ram: ram in MB per node
        :type ram: int
        """
        if not cluster:
            cluster = "{}-{}".format(os.getlogin(), int(time.time()))
        log.info("Using cluster name '%s'", cluster)

        # if specified use node names otherwise generate them
        if not nodes:
            nodes = [
                "node{}".format(i+1)
                for i in range(numberOfNodes)
            ]
        log.info("Using node names '%s'", ",".join(nodes))

        image = self.driver.get_image(imageId)
        if not image:
            log.error("Could not find image with id '%s'", imageId)
            return None
        log.info("Using image '%s'", image.name)

        # include the default OS disk in the size
        if disks:
            disks = [100] + disks
        else:
            disks = [100]
        size = self.driver.ex_get_size_by_attributes(cpus, ram, disks)
        if not size:
            log.error("Could not find size with '%d' cpus, '%d' ram and '%s' disks",
                      cpus, ram, ",".join(str(capacity for capacity in disks)))
            return None
        log.info("Using size '%s'", size.name)

        return self.driver.ex_create_cluster(
            cluster=cluster,
            image=image,
            names=nodes,
            size=size
        )

    def destroyCluster(self, *clusterNames):
        """
        Destroy clusters

        :param clusterNames: cluster names
        :type clusterNames: []
        :returns: `True` if successful, `False` otherwise
        :rtype: bool
        """
        clusters = self.driver.ex_list_clusters()
        clusterMap = {
            cluster.name: cluster
            for cluster in clusters
        }
        destroyClusters = []
        for clusterName in clusterNames:
            if clusterName in clusterMap:
                destroyClusters.append(clusterMap[clusterName])
            else:
                log.error("Could not find cluster with name '%s'", clusterName)
                return False

        return all(
            cluster.destroy()
            for cluster in destroyClusters
        )

    def destroyNode(self, *nodeNames):
        """
        Destroy nodes

        :param nodeNames: node names
        :type nodeNames: []
        :returns: `True` if successful, `False` otherwise
        :rtype: bool
        """
        nodes = self.driver.list_nodes()
        nodesMap = {
            node.name: node
            for node in nodes
        }
        destroyNodes = []
        for nodeName in nodeNames:
            if nodeName in nodesMap:
                destroyNodes.append(nodesMap[nodeName])
            else:
                log.error("Could not find node with name '%s'", nodeName)
                return False

        return all(
            self.driver.destroy_node(node)
            for node in destroyNodes
        )

    def listClusters(self):
        """
        List clusters
        """
        # TODO: add cluster name to extra portion of the nodes
        clusters = self.driver.ex_list_clusters()
        table = PrettyTable(["name", "nodes"])
        for cluster in clusters:
            table.add_row([cluster.name,
                           ",".join(sorted(cluster.nodes.keys()))])
        print(table)

    def listImages(self):
        """
        List images
        """
        images = self.driver.list_images()
        table = PrettyTable(["id", "name"])
        for image in images:
            table.add_row([image.id, image.name])
        print(table)

    def listLocations(self):
        """
        List locations
        """
        locations = self.driver.list_locations()
        table = PrettyTable(["id", "name", "long name", "city", "country"])
        for location in locations:
            table.add_row([location.id,
                           location.name,
                           location.extra.get("longName", "") if hasattr(location, "extra") else "",
                           location.extra.get("city", "") if hasattr(location, "extra") else "",
                           location.country
                          ])
        print(table)

    def listNodes(self, includePasswords=False, nodeFilter=None, outputFormat="table"):
        """
        List nodes

        :param includePasswords: include passwords in output
        :type includePasswords: bool
        :param nodeFilter: node filter
        :type nodeFilter: []
        :param outputFormat: output format
        :type outputFormat: str
        """
        nodes = self.driver.list_nodes()
        if nodeFilter:
            filteredNodes = [
                node
                for node in nodes
                if any(node.name.startswith(nodeNameFilter)
                       for nodeNameFilter in nodeFilter)
            ]
            nodes = filteredNodes

        if outputFormat == "json":
            # create nodes information
            nodesInformation = NodesInfoMap()
            nodesInformation.addNodes(nodes)
            print(nodesInformation.toJSON(includeClassInfo=True, pretty=True))

        else:
            fieldNames = ["id", "name", "public ip", "private ip", "password", 'state', 'disks']
            fields = fieldNames[:]
            # only show passwords if specifically requested
            if not includePasswords:
                fields.remove("password")
            table = PrettyTable(field_names=fieldNames, fields=fields)
            for node in nodes:
                table.add_row([node.id,
                               node.name,
                               node.public_ips[0] if node.public_ips else "",
                               node.private_ips[0] if node.private_ips else "",
                               node.extra.get("password", "unknown"),
                               node.state,
                               ",".join(str(capacity) for capacity in node.extra.get('disks'))])
            print(table)

    def listSizes(self, includeExtras=False):
        """
        List sizes

        :param includeExtras: include extra information in output
        :type includeExtras: bool
        """
        sizes = self.driver.list_sizes()
        fieldNames = ["id", "name", "cpu", "ram", "disks", "extras"]
        fields = fieldNames[:]
        # only show extras if specifically requested
        if not includeExtras:
            fields.remove("extras")
        table = PrettyTable(field_names=fieldNames, fields=fields)
        for size in sizes:
            table.add_row([size.id,
                           size.name,
                           size.cpu,
                           size.ram,
                           size.diskCapacities,
                           size.extra])
        print(table)

def main():
    """
    Main function of the cloud tooling setup
    """
    logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(name)s(%(filename)s:%(lineno)d)] - %(message)s', level=logging.INFO)

    # load drivers
    driverTypes = set([])
    try:
        import storm.drivers
        # note that getting the driver implementations implicitly triggers their registration with libcloud
        driverTypes = set([
            driver.type
            for driver in getModuleClasses(storm.drivers, baseClass=NodeDriver)
        ])
    except ImportError:
        pass
    for driverType in sorted(driverTypes):
        log.info("Found storm cloud driver '%s'", driverType)
    if not driverTypes:
        log.error("Could not find any storm cloud drivers. Please install at least one.")
        return 1

    parentParser = argparse.ArgumentParser(add_help=False)
    parentParser.add_argument("-v", "--verbose", action="count", help="display debug information")

    parser = argparse.ArgumentParser(description="A utility to help with and automate cluster setup and configuration", parents=[parentParser])
    parser.add_argument("driver", choices=driverTypes,
                        action="store", type=str, help="The name of the Storm driver to use")
    parser.add_argument("--driver-config", action="store", type=str, dest="driverConfig",
                        help="Path to the driver config file. Default is ~/.<driver>")

    commandParser = parser.add_subparsers(dest="command")

    createParser = commandParser.add_parser("create", help="create", description="type of the create parser")

    createTypeParser = createParser.add_subparsers(dest="type", title="Type", description="type of the create")

    createClusterParser = createTypeParser.add_parser("cluster", help="create cluster", parents=[parentParser])
    createClusterParser.add_argument("--config", action="store", type=argparse.FileType("r"),
                                     help="cluster configuration")
    createClusterParser.add_argument("--cluster", action="store", type=str,
                                     help="cluster name")
    createClusterParser.add_argument("--image", action="store", type=str, default=DEFAULT_IMAGE_ID,
                                     help="image to be used for the nodes")
    createClusterParser.add_argument("--nodes", action="store", type=int, default=DEFAULT_NUMBER_OF_NODES,
                                     dest="numberOfNodes",
                                     help="number of nodes")
    createClusterParser.add_argument("--disk", action="append", type=int, default=[],
                                     dest="disks",
                                     help="disk size in GB. This option can be supplied more than once. Default is 1 disk of 100 GB")
    createClusterParser.add_argument("--cpus", action="store", type=int, default=2,
                                     help="number of cpus per node")
    createClusterParser.add_argument("--ram", action="store", type=int, default=2048,
                                     help="amount of ram per node in MB")
    createClusterParser.add_argument("nodes", nargs="*", default=[], type=str,
                                     help="node names")

    destroyParser = commandParser.add_parser("destroy", help="destroy")
    destroyTypeParser = destroyParser.add_subparsers(dest="type")

    destroyClusterParser = destroyTypeParser.add_parser("cluster", help="Destroy cluster", parents=[parentParser])
    destroyClusterParser.add_argument(
        "clusters",
        nargs="+",
        default=[],
        type=str,
        help="names of clusters to destroy"
    )

    destroyNodeParser = destroyTypeParser.add_parser("node", help="Destroy node", parents=[parentParser])
    destroyNodeParser.add_argument(
        "nodes",
        nargs="+",
        default=[],
        type=str,
        help="names of nodes to destroy"
    )

    listParser = commandParser.add_parser("list", help="list")
    listTypeParser = listParser.add_subparsers(dest="type")

    listTypeParser.add_parser("clusters", help="List clusters", parents=[parentParser])

    listTypeParser.add_parser("images", help="List images", parents=[parentParser])

    listTypeParser.add_parser("locations", help="List locations", parents=[parentParser])

    listNodesParser = listTypeParser.add_parser("nodes", help="List nodes", parents=[parentParser])
    listNodesParser.add_argument("--passwords", action="store_true")
    listNodesParser.add_argument("--format", default="table", choices=["table", "json"], action="store",
                                 help="Node information format")
    listNodesParser.add_argument("--filter", action="append", default=[], help="only show nodes that start with filter")

    listSizesParser = listTypeParser.add_parser("sizes", help="List sizes", parents=[parentParser])
    listSizesParser.add_argument("--extras", action="store_true")

    args = parser.parse_args()

    # TODO: implement Driver.fromConfigFile() in the drivers instead of hard coding this
    if args.driver == "fyre":
        config = ConfigParser.ConfigParser()
        if args.driverConfig:
            config.read(os.path.expanduser(args.driverConfig))
        else:
            config.read(os.path.expanduser("~/.fyre"))
        cls = get_driver("fyre")
        driver = cls(config.get("fyre", "username"), config.get("fyre", "api_key"), config.get("fyre", "endpoint_url"), config.get("fyre", "root_password"))
        # TODO: move into fyre driver
        # disable HTTPS certificate warnings
        import requests.packages.urllib3
        requests.packages.urllib3.disable_warnings()
    elif args.driver == "softlayer":
        config = ConfigParser.ConfigParser()
        config = ConfigParser.ConfigParser()
        if args.driverConfig:
            config.read(os.path.expanduser(args.driverConfig))
        else:
            config.read(os.path.expanduser("~/.softlayer"))
        cls = get_driver("SoftLayerPythonAPI")
        driver = cls(config.get("softlayer", "username"), config.get("softlayer", "api_key"))
    elif args.driver == "local":
        cls = get_driver("local")
        driver = cls()
    else:
        raise NotImplementedError

    bolt = Bolt(driver)

    # TODO: adjust logging since this should not depend on anything specific
    logging.getLogger("storm").setLevel(logging.INFO)
    logging.getLogger("storm.thunder.client.AdvancedSSHClient").setLevel(logging.INFO)
    logging.getLogger("c4.utils").setLevel(logging.INFO)
    logging.getLogger("paramiko").setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.ERROR)
#     logging.getLogger("softlayer").setLevel(logging.ERROR)
#     logging.getLogger("SoftLayer").setLevel(logging.ERROR)

    if args.verbose > 0:
        logging.getLogger("storm").setLevel(logging.DEBUG)
        logging.getLogger("storm.thunder.client.AdvancedSSHClient").setLevel(logging.INFO)
        logging.getLogger("c4.utils").setLevel(logging.INFO)
    if args.verbose > 1:
        logging.getLogger("storm.thunder.client.AdvancedSSHClient").setLevel(logging.DEBUG)
        logging.getLogger("c4.utils").setLevel(logging.DEBUG)
    if args.verbose > 2:
        pass
#         logging.getLogger("softlayer").setLevel(logging.INFO)
#         logging.getLogger("SoftLayer").setLevel(logging.INFO)
    if args.verbose > 3:
        logging.getLogger("paramiko").setLevel(logging.INFO)
        logging.getLogger("requests").setLevel(logging.INFO)
#         logging.getLogger("softlayer").setLevel(logging.DEBUG)
#         logging.getLogger("SoftLayer").setLevel(logging.DEBUG)
    if args.verbose > 4:
        logging.getLogger("paramiko").setLevel(logging.DEBUG)
        logging.getLogger("requests").setLevel(logging.DEBUG)

    if args.command == "create":

        if args.type == "cluster":
            cluster = bolt.createCluster(
                cluster=args.cluster,
                cpus=args.cpus,
                disks=args.disks,
                imageId=args.image,
                nodes=args.nodes,
                numberOfNodes=args.numberOfNodes,
                ram=args.ram)
            if not cluster:
                return 1

        else:
            raise NotImplementedError

    elif args.command == "list":

        if args.type == "clusters":
            bolt.listClusters()

        elif args.type == "images":
            bolt.listImages()

        elif args.type == "locations":
            bolt.listLocations()

        elif args.type == "nodes":
            bolt.listNodes(includePasswords=args.passwords, nodeFilter=args.filter, outputFormat=args.format)

        elif args.type == "sizes":
            bolt.listSizes(args.extras)

        else:
            raise NotImplementedError

    elif args.command == "destroy":

        if args.type == "cluster":
            bolt.destroyCluster(*args.clusters)

        elif args.type == "node":
            bolt.destroyNode(*args.nodes)

        else:
            raise NotImplementedError

    else:
        raise NotImplementedError

    return 0

if __name__ == '__main__':
    sys.exit(main())
