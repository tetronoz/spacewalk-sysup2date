#!/usr/bin/python
from datetime import datetime
from time import sleep
import re, getpass, sys, xmlrpclib, os
from numpy import loadtxt
from collections import defaultdict
 
 
vmware_config = """#!/bin/bash
                   if [ -x /usr/bin/vmware-config-tools.pl ]; then echo "test -x /usr/bin/vmware-config-tools.pl && /usr/bin/vmware-config-tools.pl -d && /sbin/shutdown -r -t10 now && sed -i '/sed/d' /etc/rc.d/rc.local" >> /etc/rc.d/rc.local; fi"""
 
nogpg_install = """#!/bin/bash
                   /bin/rm -f /etc/yum.repos.d/*
                   /usr/bin/yum clean all
                   /usr/sbin/rhn-profile-sync
                   """
 
get_runningkernel = """#!/bin/bash
                   /bin/uname -r
                   """
 
get_lastreboot = """#!/bin/bash
                 /usr/bin/last reboot | head -n 1 | awk '{print $5" "$6" "$7" "$8}'
                 """
 
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
        #parser.add_option('-c', action='store_true', dest='cancel_pending', default=False, help='Cancel all pending jobs.')
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
        #parser.add_argument('-n', action='store_const', dest='skip_pending', const=0, help='Continue even if there are pending jobs.')
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
    already_up2date = []
    for s in servers:
        id = servers[s][0]
        pids, pnames = checkforupdates(key, id)
        if (pids) and (pnames):
            servers[s].append(pids)
            servers[s].append(pnames)
            servers_ids.append(id)
        else:
            already_up2date.append(s)
            print "Server " +str(s) + " is up to date."
 
    for up2date_server in already_up2date:
        servers.pop(up2date_server)
 
    if len(servers_ids) > 0:
        today = datetime.today()
        earliest_occurrence = xmlrpclib.DateTime(today)
 
        #Workaround for com.redhat.rhn.common.translation.TranslationException: Could not find translator for class redstone.xmlrpc.XmlRpcArray to class java.lang.Integer
        try:
            script_aid = client.system.scheduleScriptRun(key, servers_ids, "root", "root", 300, nogpg_install, earliest_occurrence)
            if script_aid:
                print("Executing a pre-update script...")
                sleep(60)
            else:
                print("Failed to run a pre-update script.")
                print("Quiting...")
                sys.exit(-1)
        except Exception:
            for sid in servers_ids:
                if (client.system.scheduleScriptRun(key, sid, "root", "root", 300, nogpg_install, earliest_occurrence)):
                    print("Executing a pre-update script...")
                else:
                    print"Failed to run a pre-update script for server with id " + str(sid)
                    print("Quiting...")
                    sys.exit(-1)
 
        for s in servers.keys():
            print("Updating " + s + "...")
            aid = doupdate(key, servers[s])
            servers[s].append(aid)
 
        postcheck(key, servers)
    else:
        print "All systems are up to date."
        sys.exit(1)
 
 
def getdetails(key, s):
    """Get system details"""
    return client.system.getDetails(key, s)
 
def checkforupdates(key, s):
    """Check for latest updates available."""
    packages = client.system.listLatestUpgradablePackages(key, s)
    osrelease = getdetails(key, s)["release"]
    pids = []
    pnames = []
    if packages:
        for p in packages:
            # Skipping pam upgrade for RHEL5
            if p['name'] == 'pam' and osrelease == "5Server":
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
    try:
        f = client.schedule.listFailedSystems(key, aid)
    except xmlrpclib.Fault:
        return False
    else:
        if len(f) > 0:
            return f[0]['server_name'].lower().partition(".")[0]
        else:
            return False
 
def list_completed_systems(key, aid):
    """"Return a server's name that has completed successfully."""
    try:
        s = client.schedule.listCompletedSystems(key,aid)
    except xmlrpclib.Fault:
        return False
    else:
        if len(s) > 0:
            return s[0]['server_name'].lower().partition(".")[0]
        else:
            return False
 
def list_pending_systems(key, aid):
    """"Returns a list of systems that have a specific action in progress. """
    try:
        p = client.schedule.listInProgressSystems(key,aid)
    except xmlrpclib.Fault:
        return False
    else:
        if len(p) > 0:
            return p[0]['server_name'].lower().partition(".")[0]
        else:
            return False
 
def getlastboot(key, sid):
    return client.system.getDetails(key, sid)['last_boot']
 
def getosastatusbyactionid(key, p_action):
    """Return OSA Dispatcher status of a system based on pending action id number."""
    server_id = client.schedule.listInProgressSystems(key, p_action)[0]['server_id']
    return client.system.getDetails(key, server_id)['osa_status']
 
def getosastatus(key, server_id):
    return client.system.getDetails(key, server_id)['osa_status']
 
def postcheck (key, s):
    """Run post checks."""
    success = 0
    failed = 0
    pending = 0
    repeat = 30
    total = len(s)
    servers_id = []
    pending_timeout = 60
    print ("\nRunning postchecks...\n")
    #print ("Sleeping for " + str(pending_timeout) + " seconds...")
 
    pending_actions = list_pending(key)
    pending_size = len(pending_actions)
 
    while pending_size > 0 and repeat >= 1:
        if pending_size > 1:
            print "There are " + str(pending_size) + " jobs pending ..."
        else:
            print "There is " + str(pending_size) + " job pending ..."
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
            if pending_size > 1:
                print ("There are " + str(len(pending_actions)) + " pending jobs.")
            else:
                print ("There is " + str(len(pending_actions)) + " pending job.")
 
    for server in s.keys():
        #print "Checking the server - " + str(server) + ", action id - " + str(s[server][3])
        if server.lower() == list_failed_systems(key, s[server][3]):
            #print "Failed action. Server - " + str(server) + ", action id - " + str(s[server][3])
            s[server].append(0)
            failed += 1
        elif server.lower() == list_completed_systems(key, s[server][3]):
            #print "Completed action. Server - " + str(server) + ", action id - " + str(s[server][3])
            s[server].append(1)
            success += 1
            servers_id.append(s[server][0])
        elif server.lower() == list_pending_systems(key, s[server][3]):
            #print "Pending action. Server - " + str(server) + ", action id - " + str(s[server][3])
            s[server].append(2)
            pending += 1
 
    print "\nScheduled: .............. %d" % total
    print "Successful: ............. %d" % success
    print "Pending.................. %d " % pending
    print "Failed: ................. %d\n" % failed
 
    if opt.reboot:
        if success > 0:
            print "Executing a pre-reboot script..."
            today = datetime.today()
            earliest_occurrence = xmlrpclib.DateTime(today)
 
            try:
                script_aid = client.system.scheduleScriptRun(key, servers_ids, "root", "root", 300, vmware_config, earliest_occurrence)
                if script_aid:
                    print("Still executing a pre-reboot script...")
                    size = len(servers_id)
                    while len(client.system.getScriptResults(key, script_aid)) != size:
                        sleep(60)
                else:
                    print("Failed to run a pre-reboot script.")
                    print ("Quiting...")
                    sys.exit(-1)
 
            except Exception:
                script_aid = []
                for sid in servers_id:
                    aid = client.system.scheduleScriptRun(key, sid, "root", "root", 300, vmware_config, earliest_occurrence)
                    if (aid):
                        script_aid.append(aid)
                    else:
                        print("Failed to run a pre-reboot script.")
                        print("Quiting...")
                        sys.exit(-1)
 
                if len(script_aid) == len(servers_id):
                    print "Pre-reboot script was executed on all servers."
 
            print "Rebooting updated systems...\n"
            for server in s.keys():
                try:
                    if s[server][4] == 1:
                        print server
                        today = datetime.today()
                        earliest_occurrence = xmlrpclib.DateTime(today)
                        client.system.scheduleReboot(key, s[server][0], earliest_occurrence)
                except IndexError:
                    pass
 
            # Waitng 15 minutes till all servers are rebooted
            sleep(900)
 
    if opt.report:
        today = datetime.today()
        report_time = xmlrpclib.DateTime(today)
        home_dir = os.path.expanduser('~/')
        fp = open(str(home_dir) + "patchreport_" + str(report_time) + ".csv", 'w')
        fp.write('Server Name,Patching status,Last Reboot,Running Kernel,Installed updates\n')
 
        for server in s.keys():
            # SW doesn't return kernel version and the last reboot time just after system si rebooted.
            # Will be using a bash script to retrieve this information
            #kernel = client.system.getRunningKernel(key, s[server][0])
            #reboottime = getlastboot(key, s[server][0])
            #reboottime_pretty = datetime.strptime(str(reboottime), "%Y%m%dT%H:%M:%S").strftime("%d %B %Y %H:%M")
            today = datetime.today()
            earliest_occurrence = xmlrpclib.DateTime(today)
            get_running_kernel_aid = client.system.scheduleScriptRun(key, s[server][0], "root", "root", 300, get_runningkernel, earliest_occurrence)
            get_lastreboot_aid = client.system.scheduleScriptRun(key, s[server][0], "root", "root", 300, get_lastreboot, earliest_occurrence)
            while list_pending_systems(key, get_running_kernel_aid) or list_pending_systems(key, get_lastreboot_aid):
                sleep(10)
            kernel = client.system.getScriptResults(key, get_running_kernel_aid)[0]['output'].rstrip()
            reboottime = client.system.getScriptResults(key, get_lastreboot_aid)[0]['output'].rstrip()
            s[server].append(reboottime)
            s[server].append(kernel)
            status = {0: 'Failed', 1: 'Success', 2: 'Pending'}
            fp.write(server + ',' + str(status[s[server][4]]) + ',' + str(reboottime) + ',' + str(kernel) + ',' + str(s[server][2]) +'\n')
        fp.close()
 
if __name__ == '__main__':
    opt = parsecli()
 
    if opt.csv and os.path.exists(opt.csv):
        try:
            data = loadtxt(opt.csv, dtype='string', delimiter=';', usecols=(0,8))
        except IOError:
            print ("Could not open " + opt.csv)
            sys.exit(-1)
        else:
            try:
                servers_input = data[:,0]
                patching_groups = data[:,1]
                servers_input = servers_input[patching_groups == opt.patching_group]
            except IndexError:
                if data.size == 2:
                    if data[1] == opt.patching_group:
                        servers_input = data[0:1]
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
            try:
                spacewalk_server = str(raw_input("Enter Server name: "))
            except KeyboardInterrupt:
                print ""
                sys.exit(-1)
            try:
                spacewalk_login = str(raw_input("Username: "))
            except KeyboardInterrupt:
                print ""
                sys.exit(-1)
            try:
                spacewalk_password = getpass.getpass("Password:")
            except KeyboardInterrupt:
                print ""
                sys.exit(-1)
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
    try:
        client, key = connect_to_spacewalk(spacewalk_server, spacewalk_login, spacewalk_password)
    except:
        print "Username or password is incorrect."
        sys.exit(-1)
 
    for server in servers_input:
        # Some servers have been registered with FQDN names.
        server = server.partition(".")[0]
        id = client.system.searchByName(key, str(server)+"$|" + str(server) + ".example.com$")
        if (id) and (getosastatus(key, id[0]['id'])) == 'online':
            #print id[0]['id']
            #print id[0]['name']
            servers_to_update[str(server)].append(id[0]['id'])
        else:
            print ("OSA service is offline. Server " + str(server) + " will be skipped.")
 
    if opt.yes:
        print ("The following servers will be updated:\n")
        #print (servers_to_update.keys())
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