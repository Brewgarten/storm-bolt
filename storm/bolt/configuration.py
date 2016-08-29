"""
Serializable configuration functionality

Cluster DSL
-----------

We support the following cluster syntax that includes optional deployments.

.. code-block:: bash

    cluster {
        name: storm-cluster
        nodes: 3
        disks: [100,100,100]
    }
    deployments: [
        ssh.AddAuthorizedKey: {
            publicKeyPath: ~/.ssh/id_rsa.pub
        }
        software.UpdateKernel
    ]

"""
import logging
import os
import re
import time

from c4.utils.hjsonutil import HjsonSerializable
from c4.utils.jsonutil import JSONSerializable
from c4.utils.logutil import ClassLogger
from storm.thunder.configuration import (DeploymentInfos,
                                         getTypedParameter)


log = logging.getLogger(__name__)

DEFAULT_CPUS = 2
DEFAULT_IMAGE_ID = "centos-7.2"
DEFAULT_NUMBER_OF_NODES = 3
DEFAULT_RAM = 2048

@ClassLogger
class ConfigurationInfo(HjsonSerializable, JSONSerializable):
    """
    Container for cluster provisioning and deployment information

    :param clusterInfo: cluster information
    :type clusterInfo: :class:`~ClusterInfo`
    :param deploymentInfos: deployments information
    :type deploymentInfos: :class:`~DeploymentInfos`
    """
    def __init__(self, clusterInfo=None, deploymentInfos=None):
        self.cluster = clusterInfo if clusterInfo else ClusterInfo()
        self.deploymentInfos = deploymentInfos if deploymentInfos else DeploymentInfos([])

    @classmethod
    def fromHjson(cls, hjsonString, objectHook=None):
        """
        Load object from the specified Hjson string

        :param cls: the class to deserialize into
        :type cls: class
        :param hjsonString: a Hjson string
        :type hjsonString: str
        :param objectHook: a function converting a Hjson dictionary
            into a dictionary containing Python objects. If ``None``
            then the default :py:meth:`fromHjsonSerializable` is used
        :type objectHook: func
        :returns: object instance of the respective class
        """
        deploymentInfos = None
        # check for deployment infos
        deployments = re.search(r"^(?P<deployments>deployments\s*:.*)", hjsonString, re.MULTILINE | re.DOTALL)
        if deployments:
            deploymentInfos = DeploymentInfos.fromHjson(deployments.group("deployments"), objectHook=objectHook)
            hjsonString = hjsonString.replace(deployments.group("deployments"), "")

        clusterInfo = ClusterInfo.fromHjson(hjsonString, objectHook=objectHook)
        return cls(clusterInfo, deploymentInfos=deploymentInfos)

class ClusterInfo(HjsonSerializable, JSONSerializable):
    """
    Cluster information

    :param name: cluster name
    :type name: str
    :param cpus: number of cpus per node
    :type cpus: int
    :param disks: list of disks capacities in GB
    :type disks: list
    :param imageId: id of the image to use for the nodes
    :type imageId: str
    :param nodes: list of node names
    :type nodes: [str]
    :param numberOfNodes: number of nodes
    :type numberOfNodes: int
    :param ram: ram in MB per node
    :type ram: int
    """
    def __init__(
            self,
            name=None,
            cpus=DEFAULT_CPUS,
            disks=None,
            imageId=DEFAULT_IMAGE_ID,
            locationId=None,
            nodes=None,
            numberOfNodes=DEFAULT_NUMBER_OF_NODES,
            ram=DEFAULT_RAM
        ):
        self.name = name if name else "{}-{}".format(os.getlogin(), int(time.time()))
        self.cpus = cpus
        # include the default OS disk in the size
        self.disks = [100] + disks if disks else [100]
        self.image = imageId
        self.location = locationId
        # if specified use node names otherwise generate them
        if nodes:
            self.nodes = nodes
            self.numberOfNodes = len(self.nodes)
        else:
            self.nodes = [
                "node{}".format(i+1)
                for i in range(numberOfNodes)
            ]
            self.numberOfNodes = numberOfNodes
        self.ram = ram

    @classmethod
    def fromHjsonSerializable(cls, hjsonDict):
        """
        Convert a dictionary from Hjson into a respective Python
        objects. By default the dictionary is returned as is.

        :param cls: the class to deserialize into
        :type cls: class
        :param hjsonDict: the Hjson dictionary
        :type hjsonDict: dict
        :returns: modified dictionary or Python objects
        """
        if "cluster" in hjsonDict:

            clusterInfoDict = hjsonDict.pop("cluster")
            clusterParameters = {
                "cpus": getTypedParameter(clusterInfoDict, "cpus", int, default=DEFAULT_CPUS),
                "disks": getTypedParameter(clusterInfoDict, "disks", [int]),
                "imageId": getTypedParameter(clusterInfoDict, "imageId", str, default=DEFAULT_IMAGE_ID),
                "locationId": getTypedParameter(clusterInfoDict, "locationId", str),
                "name": getTypedParameter(clusterInfoDict, "name", str, default="{}-{}".format(os.getlogin(), int(time.time()))),
                "ram": getTypedParameter(clusterInfoDict, "ram", int, default=DEFAULT_RAM)
            }

            disks = clusterInfoDict.get("disks", None)
            if disks:
                clusterParameters["disks"] = getTypedParameter(clusterInfoDict, "disks", [int])

            nodes = clusterInfoDict.get("nodes", None)
            if nodes:
                if isinstance(nodes, list):
                    clusterParameters["nodes"] = getTypedParameter(clusterInfoDict, "nodes", [str])
                else:
                    clusterParameters["numberOfNodes"] = getTypedParameter(clusterInfoDict, "nodes", int)

            for key, value in clusterInfoDict.items():
                cls.log.warn("Key '%s' with value '%s' is not a valid cluster config parameter", key, value)

            return cls(**clusterParameters)
        return hjsonDict
