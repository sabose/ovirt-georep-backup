#!/usr/bin/python
import ovirtsdk.api
from ovirtsdk.xml import params
from ovirtsdk.infrastructure import errors
import sys
import time
import argparse
import ConfigParser
import logging
import subprocess
from cpopen import CPopen
import io
import select
import threading
from StringIO import StringIO
from weakref import proxy

_georepScheduleCmd = ["/usr/bin/python", "/usr/share/glusterfs/scripts/schedule_georep.py"]
_SNAPSHOT_NAME = "GLUSTER-Geo-rep-snapshot"


class VMSnapshot():
    def getVM(self):
        return self.vm

    def getSnapshot(self):
        return self.snapshot

    def __init__(self, vm, snapshot):
       self.vm = vm
       self.snapshot = snapshot

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
    # p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    (out, err) = p.communicate(data)

    if out is None:
        # Prevent splitlines() from barfing later on
        out = ""

    logger.info("%s: <err> = %s; <rc> = %d",
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

def main(args):
    retcode = 0
    if (args.config):
        # Read config file
        config = ConfigParser.ConfigParser()
        config.read(args.config)
        server = config.get('GENERAL', 'server')
        username = config.get('GENERAL', 'user_name')
        password = config.get('GENERAL', 'password')

    time_start = int(time.time())
    # Connect to server
    try:
        connect(server, username, password)
    except Exception as e:
        logger.error("Error:" + str(e))
        exit(1)

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
                # vms_to_commit.append(VMSnapshot(vm, snapshot))
                vms_to_commit.append({'vm': vm, 'snapshot': snapshot})
                logger.debug("Added snapshot for vm: " + vm_.name)
        except Exception as e:
            logger.error("Error:" + str(e))

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

    for vm_to_commit in vms_to_commit:
        # Block commit VMs
        try:
            vm = vm_to_commit['vm']
            logger.info("Delete snapshot for: " + vm.name)
            snapshot = vm_to_commit['snapshot']

            # vm.commit_snapshot() - not working  "Cannot revert to Snapshot. VM's Snapshot does not exist."
            snapshot.delete()
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
