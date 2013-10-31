#!/usr/bin/python
from datetime import datetime
import re, getpass, sys, xmlrpclib, os
from numpy import genfromtxt
 
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
        parser.add_option('-g', action='store', dest='patching_group', help='Patching group to use. Should be one of the following: PROD1, PROD2, UAT1, UAT2')
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
        return options
    else:
        parser = argparse.ArgumentParser(description='Update Linux servers using Spacewalk API.')
        parser.add_argument('-f', help='Path to a CSV file which contains names of the servers to update.')
        parser.add_argument('-y', action='store_const', const=0, help='Auto answers \'yes\' to all questions.')
        parser.parse_args()
 
if __name__ == '__main__':
    opt = parsecli()
 
    if opt.csv and os.path.exists(opt.csv):
        try:
            data = genfromtxt(opt.csv, dtype='string', delimiter=';')
        except IOError:
            print ("Could not open " + opt.csv)
            sys.exit(-1)
        else:
            servers = data[:,0]
            patching_groups = data[:,1]
 
            print(servers)
            print(patching_groups)
            print (servers[patching_groups == 'UAT1'])
 
    servers_to_update = {}
    # Building a path to the configuration file
    conf_dir = os.path.expanduser('~/.spacecmd')
    config = os.path.join(conf_dir, 'config')
 
    # Opening and parsing the configuration file
    try:
        fp = open(config, "r")
    except IOError:
        SPACEWALK_SERVER = str(raw_input("Enter Server name: "))
        SPACEWALK_LOGIN = str(raw_input("Username: "))
        SPACEWALK_PASSWORD = getpass.getpass("Password:")
    else:
        for line in fp.readlines():
            m = re.search(r'server=(.+)', line, re.I)
            if m:
                SPACEWALK_SERVER = m.group(1)
            m = re.search(r'username=(.+)', line, re.I)
            if m:
                SPACEWALK_LOGIN = m.group(1)
            m = re.search(r'password=(.+)', line, re.I)
            if m:
                SPACEWALK_PASSWORD = m.group(1)
        fp.close()
 
    if len(sys.argv) < 2:
        print ("Server list is empty.")
        sys.exit(-1)
 
    SPACEWALK_URL = "http://" + SPACEWALK_SERVER + "/rpc/api"
    client = xmlrpclib.Server(SPACEWALK_URL, verbose=0)
    key = client.auth.login(SPACEWALK_LOGIN, SPACEWALK_PASSWORD)
 
    for server in sys.argv[1:]:
        id = client.system.searchByName(key, server)
        if id:
            print (id[0]['id'])
            serversToUpdate[server] = id[0]['id']
 
    today = datetime.today()
    earliest_occurrence = xmlrpclib.DateTime(today)
 
    for server in serversToUpdate:
        packages = client.system.listLatestAvailablePackage(key, serversToUpdate[server])
        if packages:
            for p in packages:
                print(p['name'])
 
    client.auth.logout(key)