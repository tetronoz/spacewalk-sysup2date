from datetime import datetime
import re, getpass, sys, xmlrpclib, os
from numpy import loadtxt
from collections import defaultdict
 
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
        parser.add_option('-g', action='store', dest='patching_group', help='Patching group to use. Should be one of the following: MSK.PROD1, MSK.PROD2, MSK.UAT1, MSK.UAT2')
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
        parser.add_argument('-y', action='store_const', const=0, help='Auto answers \'yes\' to all questions.')
        parser.parse_args()
 
def connect_to_spacewalk(spacewalk_server, spacewalk_login, spacewalk_password):
    """Connect to SW server and return key and client for further use."""
    spacewalk_url = "http://" + spacewalk_server + "/rpc/api"
    client = xmlrpclib.Server(spacewalk_url, verbose=0)
    key = client.auth.login(spacewalk_login, spacewalk_password)
 
    return client, key
 
def prepareupdate(servers, key):
    """Update servers"""
    for s in servers:
        id = servers[s][0]
    pids, pnames = checkforupdates(id, key)
    servers[s].append(pids)
    servers[s].append(pnames)
    print servers
    for s in servers.keys():
        print("Updating " + s + "...")
        aid = doupdate(key, servers[s])
        print (aid)
 
    return 0
 
def checkforupdates(s,key):
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
    today = datetime.today()
    earliest_occurrence = xmlrpclib.DateTime(today)
    action_id = client.system.schedulePackageInstall(key,s[0], s[1], earliest_occurrence)
    return action_id
 
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
        prepareupdate(servers_to_update,key)
    else:
        print ("The following server(s) will be updated:\n")
        print (servers_to_update.keys())
        answer = raw_input("Continue (Y/N): ")
        if answer == 'y' or answer == 'Y':
            prepareupdate(servers_to_update, key)
        else:
            print("Quiting...\n")
            sys.exit(0)
 
    client.auth.logout(key)