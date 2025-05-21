import boto3
from ec2_metadata import ec2_metadata
from .exceptions.unsupported_format import EC2InstanceNotFoundException
from .utils import logger
from requests.exceptions import RequestException
class EnvironmentNameProvider:
    def get_instance_name(self, fid):
        """
        When given an instance ID as str e.g. 'i-1234567', return the instance 'Name' from the name tag.
        :param fid:
        :return:
        """
        try:
            logger.debug("Starting the get_instance_name function.")
            ec2 = boto3.resource("ec2")
            ec2instance = ec2.Instance(fid)
            instancename = ""
            for tags in ec2instance.tags:
                if tags["Key"] == "Name":
                    instancename = tags["Value"]
            logger.debug(f"Instance name extracted: {instancename}")
            logger.debug("Exiting the get_instance_name function.")
            return instancename
        except Exception as e:
            logger.error(f"An error occurred while getting instance name: {e}")
            raise e
    def get_environment(self):
        """
        This method is responsible for providing the name of the EC2 instance,
        which can include names like "dev1," "dev2," "demo," and "qa1" for projects.
        Returns:
            str: returns the environment name
        """
        try:
            logger.debug("Starting the get_environment function.")
            instance_id = ec2_metadata.instance_id
            instance_name = self.get_instance_name(instance_id)
            env_details = instance_name.split("-")
            environment_name = str.lower(env_details[0])
            logger.debug(f"Environment name extracted: {environment_name}")
            logger.debug("Exiting the get_environment function.")
            return environment_name
        except Exception as e:
            logger.error(
                f"An error occurred while trying to extract the name of the EC2 instance: {e}"
            )
            raise e
