from datetime import datetime
from time import sleep
import re, getpass, sys, xmlrpclib, os
from numpy import loadtxt
from collections import defaultdict
 
 
vmware_config = """#!/bin/bash
                   if [ -x /usr/bin/vmware-config-tools.pl ]; then echo "test -x /usr/bin/vmware-config-tools.pl && /usr/bin/vmware-config-tools.pl -d && /sbin/shutdown -r -t10 now && sed -i '/sed/d' /etc/rc.d/rc.local" >> /etc/rc.d/rc.local; fi"""
 
 
nogpg_install = """#!/bin/bash
                   /usr/sbin/rhn-profile-sync
                   /bin/rpm -qa --qf "%{siggpg} %{name}\n" | grep "^(none)" | grep -v gpg |awk '{
                   pkg=pkg" "$2;
                   } END {
                   print pkg;
                   system("/usr/bin/yum --nogpgcheck -y update "pkg"");
                   }'"""
 
# Python 2.6 doesn't support argparse
try:
    import argparse
except ImportError:
    from optparse import OptionParser
 
def vararg_callback(option, opt_str, value, parser):
    """http://docs.python.org/2/library/optparse.html#callback-example-6-variable-arguments"""
    assert value is None
    value = []
 
    def floatable(str):
        try:
            float(str)
            return True
        except ValueError:
            return False
 
    for arg in parser.rargs:
    # stop on --foo like options
        if arg[:2] == "--" and len(arg) > 2:
            break
        # stop on -a, but not on -3 or -3.0
        if arg[:1] == "-" and len(arg) > 1 and not floatable(arg):
            break
        value.append(arg)
 
    del parser.rargs[:len(value)]
    setattr(parser.values, option.dest, value)
 
def parsecli():
    """Parse CLI arguments and return an object containing values for all of our options."""
    if sys.version_info[0] < 3 and sys.version_info[1] < 7:
        parser = OptionParser()
        parser.add_option('-f', action='store', dest='csv', help='Path to a CSV file with names of the servers to update.')
        parser.add_option('-y', action='store_true', dest='yes', default=False, help='Auto answers \'yes\' to all questions.')
        parser.add_option('-n', action='store_true', dest='skip_pending', default=False, help='Continue even if there are pending jobs.')
        parser.add_option('-g', action='store', dest='patching_group', help='Patching group to use. Should be one of the following: MSK.PROD1, MSK.PROD2, MSK.UAT1, MSK.UAT2')
        parser.add_option('-o', action='store_true', dest='report', default=False, help='Generate CSV with a report or prints to stdout otherwise.')
        parser.add_option('-r', action='store_true', dest='reboot', default=False, help='Reboot successfully updated systems.')
        parser.add_option('-s', action='callback', callback=vararg_callback, dest="servers_list", help='Space separated list of servers to update.')
        (options, args) = parser.parse_args()
        if options.servers_list and options.csv:
            print("\n-s and -f options are mutual exclusive.\n")
            parser.print_help()
            sys.exit(-1)
        if not options.servers_list and not options.csv:
            print("\nEither -s or -f options must be specified.\n")
            parser.print_help()
            sys.exit(-1)
        if options.csv and not options.patching_group:
            print("\nPatching group definition is missing.\n")
            parser.print_help()
            sys.exit(-1)
        return options
    else:
        parser = argparse.ArgumentParser(description='Update Linux servers using Spacewalk API.')
        parser.add_argument('-f', help='Path to a CSV file which contains names of the servers to update.')
        parser.add_argument('-y', action='store_const', dest='yes', const=0, help='Auto answers \'yes\' to all questions.')
        parser.add_argument('-n', action='store_const', dest='skip_pending', const=0, help='Continue even if there are pending jobs.')
        parser.add_argument('-g', action='store', dest='patching_group', help='Patching group to use. Should be one of the following: MSK.PROD1, MSK.PROD2, MSK.UAT1, MSK.UAT2')
        parser.add_argument('-s', help='Space separated list of servers to update.')
        parser.parse_args()
 
def connect_to_spacewalk(spacewalk_server, spacewalk_login, spacewalk_password):
    """Connect to SW server and return key and client for further use."""
    spacewalk_url = "http://" + spacewalk_server + "/rpc/api"
    client = xmlrpclib.Server(spacewalk_url, verbose=0)
    key = client.auth.login(spacewalk_login, spacewalk_password)
 
    return client, key
 
#
# {'server name': [server id, [package id, package id, ... ], [package name, package name, ...], action id, flag]}
# flag: 0 - failed, 1 - success, 2 - pending
# {'smsk01mg02': [1000010074, [13154, 13154], ['kernel-2.6.18-371.el5-x86_64', 'kernel-2.6.18-371.el5-x86_64'], 8856, 1]}
#
 
def prepareupdate(key, servers):
    """Update servers"""
    servers_ids = []
    for s in servers:
        id = servers[s][0]
        pids, pnames = checkforupdates(key, id)
        if (pids):
            servers[s].append(pids)
            servers[s].append(pnames)
            servers_ids.append(id)
 
    today = datetime.today()
    earliest_occurrence = xmlrpclib.DateTime(today)
   script_aid = client.system.scheduleScriptRun(key, servers_ids, "root", "root", 300, nogpg_install, earliest_occurrence)
    if script_aid:
        print("Executing a pre-update script...")
        sleep(60)
    else:
        print("Failed to run a pre-update script.")
        print("Quiting...")
        sys.exit(-1)
 
    for s in servers.keys():
        print("Updating " + s + "...")
        aid = doupdate(key, servers[s])
        servers[s].append(aid)
 
    postcheck(key, servers)
 
def checkforupdates(key, s):
    """Check for latest updates available."""
    packages = client.system.listLatestUpgradablePackages(key, s)
    pids = []
    pnames = []
    if packages:
        for p in packages:
            # Skipping pam upgrade
            if p['name'] == 'pam':
                continue
            pnames.append(p['name'] + "-" + p['to_version'] + "-" + p['to_release'] + "-" + p['to_arch'])
            pids.append(p['to_package_id'])
    return pids, pnames
 
def doupdate (key, s):
    """Call system.schedulePackageInstall to install available updates."""
    today = datetime.today()
    earliest_occurrence = xmlrpclib.DateTime(today)
    action_id = client.system.schedulePackageInstall(key, s[0], s[1], earliest_occurrence)
    return action_id
 
def list_pending(key):
    "Return pending actions info."
    return client.schedule.listInProgressActions(key)
 
def list_failed_systems(key, aid):
    """Return a server's name that has failed an action id."""
    f = client.schedule.listFailedSystems(key, aid)
    if len(f) > 0:
        return f[0]['server_name']
    else:
        return False
 
def list_completed_systems(key, aid):
    """"Return a server's name that has completed successfully."""
    s = client.schedule.listCompletedSystems(key,aid)
    if len(s) > 0:
        return s[0]['server_name']
    else:
        return False
 
def getlastboot(key, sid):
    return client.system.getDetails(key, sid)['last_boot']
 
def postcheck (key, s):
    """Run post checks."""
    success = 0
    failed = 0
    pending = 0
    repeat = 5
    total = len(s)
    servers_id = []
    pending_timeout = 10
    print ("\nRunning postchecks...\n")
    print ("Sleeping for " + str(pending_timeout) + " seconds...")
 
    pending_actions = list_pending(key)
    pending_size = len(pending_actions)
 
    while pending_size > 0 and repeat >= 1:
        sleep(pending_timeout)
        pending_actions = list_pending(key)
        pending_size = len(pending_actions)
        repeat -= 1
 
    if pending_size > 0:
        # Checking for pending actions for the last time
        pending_actions = list_pending(key)
        pending_size = len(pending_actions)
        if pending_size > 0:
            pending_actions = [pending_actions[i]['id'] for i in range(pending_size) if (pending_actions[i]['inProgressSystems']) > 0]
            print ("There are " + str(len(pending_actions)) + " pending jobs.")
            if (opt.skip_pending):
                print ("Quiting...")
                sys.exit(-1)
            else:
                print ("Continuing...")
 
    for server in s.keys():
        if server == list_failed_systems(key, s[server][3]):
            s[server].append(0)
            failed += 1
        elif server == list_completed_systems(key, s[server][3]):
            s[server].append(1)
            success += 1
            servers_id.append(s[server][0])
        else:
            s[server].append(2)
            pending += 1
 
 
    print "\nScheduled: .............. %d" % total
    print "Successful: ............. %d" % success
    print "Pending: ................ %d " % pending
    print "Failed: ................. %d\n" % failed
 
    if opt.reboot:
        print "Executing a pre-reboot script..."
        today = datetime.today()
        earliest_occurrence = xmlrpclib.DateTime(today)
        script_aid = client.system.scheduleScriptRun(key, servers_id, "root", "root", 300, vmware_config, earliest_occurrence)
        if script_aid:
            print("Still executing a pre-reboot script...")
            size = len(servers_id)
            while len(client.system.getScriptResults(key, script_aid)) != size:
                sleep(60)
        else:
            print("Failed to run a pre-reboot script.")
            print ("Quiting...")
            sys.exit(-1)
        print "Rebooting updated systems...\n"
        for server in s.keys():
            if s[server][4] == 1:
                print server
                today = datetime.today()
                earliest_occurrence = xmlrpclib.DateTime(today)
                #client.system.scheduleReboot(key, s[server][0], earliest_occurrence)
 
        # Waitng 10 minutes till all servers are rebooted
        sleep(60)
 
        if opt.report:
            today = datetime.today()
            report_time = xmlrpclib.DateTime(today)
            fp = open("patchreport_" + str(report_time) + ".csv", 'w')
            fp.write('Server Name,Patching status,Last Reboot,Running Kernel,Installed updates\n')
 
        for server in s.keys():
            reboottime = getlastboot(key, s[server][0])
            kernel = client.system.getRunningKernel(key, s[server][0])
            reboottime_pretty = datetime.strptime(str(reboottime), "%Y%m%dT%H:%M:%S").strftime("%d %B %Y %H:%M")
            s[server].append(reboottime_pretty)
            s[server].append(kernel)
            status = {0: 'Failed', 1: 'Success', 2: 'Pending'}
            if opt.report:
                fp.write(server + ',' + str(status[s[server][4]]) + ',' + reboottime_pretty + ',' + kernel + ',' + str(s[server][2]) +'\n')
            else:
                print("Patching:............. %s") % str(status[s[server][4]])
                print("Last rebooted:........ %s" % reboottime_pretty)
                print("Running kernel:....... %s") % kernel
                print("Installed updates:.....%s") % str(s[server][2])
 
        if  opt.report:
            fp.close()
 
if __name__ == '__main__':
    opt = parsecli()
 
    if opt.csv and os.path.exists(opt.csv):
        try:
            data = loadtxt(opt.csv, dtype='string', delimiter=';', skiprows=1, usecols=(1,9))
        except IOError:
            print ("Could not open " + opt.csv)
            sys.exit(-1)
        else:
            servers_input = data[:,0]
            patching_groups = data[:,1]
 
            servers_input = servers_input[patching_groups == opt.patching_group]
    else:
        servers_input = opt.servers_list
 
    if len(servers_input) > 0:
        # Building a path to the configuration file
        conf_dir = os.path.expanduser('~/.spacecmd')
        config = os.path.join(conf_dir, 'config')
        # Opening and parsing the configuration file
        try:
            fp = open(config, "r")
        except IOError:
            spacewalk_server = str(raw_input("Enter Server name: "))
            spacewalk_login = str(raw_input("Username: "))
            spacewalk_password = getpass.getpass("Password:")
        else:
            for line in fp.readlines():
                m = re.search(r'server=(.+)', line, re.I)
                if m:
                    spacewalk_server = m.group(1)
                m = re.search(r'username=(.+)', line, re.I)
                if m:
                    spacewalk_login = m.group(1)
                m = re.search(r'password=(.+)', line, re.I)
                if m:
                    spacewalk_password = m.group(1)
            fp.close()
 
    servers_to_update = defaultdict(list)
    client, key = connect_to_spacewalk(spacewalk_server, spacewalk_login, spacewalk_password)
 
    for server in servers_input:
        id = client.system.searchByName(key, str(server))
        if id:
            servers_to_update[str(server)].append(id[0]['id'])
 
    if opt.yes:
        print ("The following servers will be updated:\n")
        print (servers_to_update.keys())
        prepareupdate(key, servers_to_update)
    else:
        print ("The following server(s) will be updated:\n")
        print (servers_to_update.keys())
        answer = raw_input("Continue (Y/N): ")
        if answer == 'y' or answer == 'Y':
            prepareupdate(key, servers_to_update)
        else:
            print("Quiting...\n")
            sys.exit(0)
 
    client.auth.logout(key)