# ovirt-georep-backup #

This is a helper script to backup VM image files that are running on gluster volumes to a remote or central site. The script uses VM live snapshots and gluster geo-replication. 

Since gluster 3.7.9, we have a script packaged with gluster rpm that helps with scheduling geo-replication between volumes. However, running geo-rep on a volume hosting VM images may not provide consistency for VM images as ongoing I/O post checkpoint time could also be transferred to slave volume. To ensure that VM disk images are consistent, we can take a qcow snapshot prior to replicating. This script helps with this orchestration. 

## Assumptions ##
* You have an oVirt instance running that uses gluster volume as storage domain to store the VM images
* Geo-replication session is created between the gluster volume (that's used as storage domain) and another gluster volume at the central site. For further information on geo-replication, refer https://gluster.readthedocs.org/en/latest/Administrator%20Guide/Geo%20Replication/

## Pre-requisites ##
* glusterfs >= 3.7.9
* oVirt >= 3.6.0
* ovirt-sdk-python >= 3.6.0 (required on the server where script is run)

## Usage ##
    vmbackup.py <mastervolume> <slavehost> <slavevolumename> -c <path to backup.cfg> --timeout <in-minutes>

## What the script does ##
* Establishes connection to oVirt based on credentials provided in config file
* Queries for list of running VMs (TBD: can be enhanced to look either for user input VMs or VMs that have disks on provided gluster volume)
* Creates a qcow snapshot of all running VMs to ensure that data is quiesced and in a consistent state at the backup site. Any data that is written post this checkpoint, though transferred to backup site will be discarded as it is in a separate overlay image file.
* Executes the georep_scheduler script that:
   * creates a checkpoint
   * starts geo-replication, copying files from master volume (storage domain) to the slave volume located at central backup site
   * pauses the geo-replication session once checkpoint is completed
* Mounts the slave volume from backup site and deletes the external snapshot image file (overlay image) that was created as part of script
* Deletes the created snapshots via oVirt API that calls live merge of image
* On completion, posts an event to oVirt engine. The event is an Alert with error message in case the process did not complete successfully. Administrator can subscribe to external events to receive the events via email.

