#!/usr/bin/python
import ovirtsdk.api
from ovirtsdk.xml import params
import os
import fnmatch
import sys
import time
import argparse
import ConfigParser
import logging
import subprocess
from cpopen import CPopen
from contextlib import contextmanager
import tempfile

_georepScheduleCmd = ["/usr/bin/python", "/usr/share/glusterfs/scripts/schedule_georep.py"]
_SNAPSHOT_NAME = "GLUSTER-Geo-rep-snapshot"
SLAVE_MOUNT_LOG_FILE = ("/var/log/glusterfs/geo-replication"
                          "/dr_slave.mount.log")

class VMSnapshot():
    def getVM(self):
        return self.vm

    def getSnapshot(self):
        return self.snapshot

    def __init__(self, vm, snapshot):
       self.vm = vm
       self.snapshot = snapshot

def findImgPaths(imgid, path):
    pattern = imgid + '*'
    for root, dirs, files in os.walk(path):
        for filename in fnmatch.filter(files, pattern):
            yield os.path.join(root, filename)

def cleanup(hostname, volname, mnt):
    """
    Unmount the Volume and Remove the temporary directory
    """
    (ret, out, err) = execCmd(["umount", mnt])
    if ret !=0:
        logger.error("Unable to Unmount Gluster Volume "
            "{0}:{1}(Mounted at {2})".format(hostname, volname, mnt))
    (ret, out, err) = execCmd(["rmdir", mnt])
    if ret !=0:
        logger.error("Unable to Remove temp directory "
            "{0}".format(mnt))


@contextmanager
def glustermount(hostname, volname):
    """
    Context manager for Mounting Gluster Volume
    Use as
        with glustermount(HOSTNAME, VOLNAME) as MNT:
            # Do your stuff
    Automatically unmounts it in case of Exceptions/out of context
    """
    mnt = tempfile.mkdtemp(prefix="drcleanup_")
    logger.debug("MNT:" + mnt)
    (ret, out, err) = execCmd(["/usr/sbin/glusterfs",
                             "--volfile-server", hostname,
                             "--volfile-id", volname,
                             "-l", SLAVE_MOUNT_LOG_FILE,
                             mnt])
    if ret != 0:
        logger.error("Unable to mount Gluster Volume "
                     "{0}:{1} at {2}".format(hostname, volname, mnt))
    if os.path.ismount(mnt):
        yield mnt
    else:
        logger.info("Unable to Mount Gluster Volume "
                     "{0}:{1}".format(hostname, volname))
    cleanup(hostname, volname, mnt)


def execCmd(command, cwd=None, data=None, raw=False,
            env=None, sync=True, deathSignal=0, childUmask=None):
    """
    Executes an external command,
    IMPORTANT NOTE: the new process would receive `deathSignal` when the
    controlling thread dies, which may not be what you intended: if you create
    a temporary thread, spawn a sync=False sub-process, and have the thread
    finish, the new subprocess would die immediately.
    """

    cmdline = repr(subprocess.list2cmdline(command))
    logger.info("%s (cwd %s)", cmdline, cwd)

    p = CPopen(command, close_fds=True, cwd=cwd, env=env,
               deathSignal=deathSignal, childUmask=childUmask)

    (out, err) = p.communicate(data)

    if out is None:
        # Prevent splitlines() from barfing later on
        out = ""

    logger.debug("%s: <err> = %s; <rc> = %d",
                        {True: "SUCCESS", False: "FAILED"}[p.returncode == 0],
                 repr(err), p.returncode)

    if not raw:
        out = out.splitlines(False)
        err = err.splitlines(False)

    return (p.returncode, out, err)

def parse_input():
    parser = argparse.ArgumentParser()
    parser.add_argument("mastervol", help="Master Volume Name")
    parser.add_argument("slave",
                        help="SLAVEHOST or root@SLAVEHOST "
                        "or user@SLAVEHOST",
                        metavar="SLAVE")
    parser.add_argument("slavevol", help="Slave Volume Name")
    parser.add_argument("-i", "--interval", help="Interval in Seconds. "
                        "Wait time before each status check",
                        type=int, default=10)
    parser.add_argument("-c", "--config", action="store",
                        required=True,
                        help="Path to the config file")
    parser.add_argument("-t", "--timeout", help="Timeout in minutes. Script will "
                        "stop Geo-replication if Checkpoint is not complete "
                        "in the specified timeout time", type=int,
                        default=0)

    args = parser.parse_args()
    return args

def wait_for_snapshot_deletion(vm, snapshotid):
    while True:
        snapshot = vm.snapshots.get(id=snapshotid)
        if snapshot is not None:
            logger.debug ("Snapshot status: " + snapshot.get_snapshot_status())
            time.sleep(10)
        else:
            logger.error ("Snapshot deleted for VM: " + vm.name)
            break

def main(args):
    retcode = 0
    if (args.config):
        # Read config file
        config = ConfigParser.ConfigParser()
        config.read(args.config)
        server = config.get('GENERAL', 'server')
        username = config.get('GENERAL', 'user_name')
        password = config.get('GENERAL', 'password')
    if not server or not username or not password:
        logger.error("Server credentials not provided")
        sys.exit("Server credentials not provided")

    time_start = int(time.time())
    # Connect to server
    try:
        connect(server, username, password)
        logger.debug("connected to server: " + server)
    except Exception as e:
        logger.error("Error:" + str(e))
        sys.exit(1)

    vms=api.vms.list(max=100)

    vms_to_commit = []
    for vm_ in vms:
        try:
            # Get the VM
            vm = api.vms.get(vm_.name)
            if vm.status.state == 'up' and vm.name != 'HostedEngine':
                logger.info("Adding snapshot for: " + vm_.name )
                snapshot = vm.snapshots.add(params.Snapshot(description=_SNAPSHOT_NAME))
                logger.debug("snapshot: " + snapshot.get_id())
                vms_to_commit.append({'vm': vm, 'snapshot': snapshot})
                logger.debug("Added snapshot for vm: " + vm_.name)
        except Exception as e:
            logger.error("Error:" + str(e))

    diskimgs_to_del = []
    for vm_to_commit in vms_to_commit:
        try:
            # Get the VM
            vmcached = vm_to_commit['vm']
            vm = api.vms.get(vmcached.name)
            snapshotid = vm_to_commit['snapshot'].get_id()
            logger.debug("Refreshed vm object for: " + vm.name + ":" + snapshotid)
            while True:
                snapshot = vm.snapshots.get(id=snapshotid)
                if snapshot is not None:
                    if snapshot.get_snapshot_status() == 'ok':
                        logger.info("Snapshot created for VM :" + vm.name)
                        # get the overlay image id to delete at slave
                        overlaydisks = vm.disks.list()
                        for disk in overlaydisks:
                            logger.debug("DISK:" + disk.get_id())
                            dskImage = disk.get_image_id()
                            logger.debug("DISK IMAGE:" + dskImage)
                            diskimgs_to_del.append(dskImage)
                        break
                    else:
                        logger.debug ("Snapshot status: " + snapshot.get_snapshot_status())
                        time.sleep(10)
                else:
                    logger.error ("Snapshot not retrieved for vm: " + vm.name)
                    break

        except Exception as e:
            logger.error("Error:" + str(e))

    # call geo-rep scheduler
    cmd = _georepScheduleCmd + [args.mastervol, args.slave, args.slavevol, "--interval", str(args.interval),
                                 "--timeout", str(args.timeout)]
    ret, out, err = execCmd(cmd)
    # Post schedule successful exit - block commit all VMs
    if ret != 0:
        logger.error("Error:" + str(out) + ":" + '.'.join(err))
        retcode = 1
    else:
        # delete overlay images from slave
        with glustermount(args.slave, args.slavevol) as mnt:
            # find overlay image path (returns .lease and .meta files too)
            for diskImg in diskimgs_to_del:
                imgPaths = findImgPaths(diskImg, mnt)
                for imgPath in imgPaths:
                    logger.debug("IMG PATH:" + imgPath)
                    os.remove(imgPath)

    for vm_to_commit in vms_to_commit:
        # Block commit VMs
        try:
            vm = vm_to_commit['vm']
            logger.debug("Deleting snapshot for: " + vm.name)
            snapshot = vm_to_commit['snapshot']
            # live merge
            snapshot.delete()
            # wait for snapshot deletion to complete
            wait_for_snapshot_deletion(vm, snapshot.get_id())
            logger.debug("Deleted snapshot {0} for vm {1}".format(snapshot.get_name(), vm.name))
        except Exception as e:
            logger.error("Error:" + str(e))

    time_end = int(time.time())
    time_diff = (time_end - time_start)
    time_minutes = int(time_diff / 60)
    time_seconds = time_diff % 60

    logger.info("Duration: " + str(time_minutes) + ":" + str(time_seconds) + " minutes")

    # Disconnect from the server
    api.disconnect()
    sys.exit(retcode)

def connect(url, username, password):
    global api
    api = ovirtsdk.api.API(
        url=url,
        username=username,
        password=password,
        insecure=True,
        debug=False
    )

if __name__ == "__main__":
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    args = parse_input()
    main(args)
