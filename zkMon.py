#!/usr/bin/python2.7
"""
This script monitors the zookeeper service and its metrics exposed through stat and mntr commands. 
Collected metrics are sent to graphite
"""
import re
import os
import time
import sys
import socket
import subprocess

service_conf_path="/u/users/s0s029n/zkmon/conf"
zk_conf_path="/u/users/s0s029n/zookeeper/conf"
zk_pid_file_path="/var/run/zookeeper/zookeeper_server.pid"
CARBON_SERVER = 'graphitehost'
CARBON_PORT = 2003
metrics_prefix="prod.aws.app.zookeeper.kafka1.".format(socket.gethostname())


def serviceCheck(pid_path):
    pid_file=pid_path
    if not os.path.exists(pid_file):
        print("zookeeper pid file {} does not exist".format(pid_file))
        return 0
    with open(pid_file, 'r') as pid:
        pid_num=pid.read()
    if pid_num and os.path.exists("/proc/"+pid_num):
        return 1
    else:
        return 0


def getStatLocal():
    cmd1=subprocess.Popen(["echo", "stat"], stdout=subprocess.PIPE)
    cmd2=subprocess.Popen(["nc", "localhost", "2181"], stdin=cmd1.stdout, stdout=subprocess.PIPE)
    return cmd2.communicate()


def getMntrLocalLeader():
    cmd1=subprocess.Popen(["echo", "mntr"], stdout=subprocess.PIPE)
    cmd2=subprocess.Popen(["nc", "localhost", "2181"], stdin=cmd1.stdout, stdout=subprocess.PIPE)
    return cmd2.communicate()


def getHostRole():
    cmd1=subprocess.Popen(["echo", "stat"], stdout=subprocess.PIPE)
    cmd2=subprocess.Popen(["nc", "localhost", "2181"], stdin=cmd1.stdout, stdout=subprocess.PIPE)
    cmd3=subprocess.Popen(["grep", "Mode"],stdin=cmd2.stdout, stdout=subprocess.PIPE)
    mode=cmd3.communicate()
    print(mode)
    return mode[0].split(":")[1].strip().lower()


def prepareFollowerMetrics():
    reg_str=''
    follower_metrics={}
    follower_metrics_collection_list=['Connections','Outstanding','Latency']
    myop1 = getStatLocal()
    for i in follower_metrics_collection_list:
        if reg_str=='':
            reg_str=reg_str+'^'+i
        else:
            reg_str=reg_str+"|^"+i
    for i in myop1[0].split("\n"):
        if i and re.match(reg_str, i):
            key,value = i.split(':')
            if key.split()[0].strip()=='Latency':
                latency_min,latency_avg,latency_max=value.split('/')
                follower_metrics['latency_min']=int(latency_min)
                follower_metrics['latency_avg']=int(latency_avg)
                follower_metrics['latency_max']=int(latency_max)
            else:
                follower_metrics[key.split()[0].strip().lower()]=int(value)
    return follower_metrics


def prepareLeaderMetrics():
    reg_str=''
    leader_metrics={}
    collect_values=(
     "zk_avg_latency\n"
     "zk_max_latency\n"
     "zk_min_latency\n"
     "zk_packets_received\n"
     "zk_packets_sent\n"
     "zk_num_alive_connections\n"
     "zk_outstanding_requests\n"
     "zk_znode_count\n"
     "zk_watch_count\n"
     "zk_approximate_data_size\n"
     "zk_open_file_descriptor_count\n"
     "zk_max_file_descriptor_count\n"
     "zk_followers\n"
     "zk_synced_followers\n"
     "zk_pending_syncs")
    leader_metrics_collection_list=collect_values.split("\n")
    myop1=getMntrLocalLeader()
    for i in leader_metrics_collection_list:
        if reg_str=='':
            reg_str=reg_str+'^'+i
        else:
            reg_str=reg_str+"|^"+i
    for i in myop1[0].split("\n"):
        if i and re.match(reg_str, i):
            key,value=i.split()
            leader_metrics[key.strip()]=int(value.strip())
    return leader_metrics



def sendToGraphite(carbon_server, carbon_port, message):
    """
     each message should end with a \n for the send to work
    """
    sock = socket.socket()
    sock.connect( (carbon_server, carbon_port) )
    print "{}".format(message), #, to avoid new line printing
    sock.send(message)
    sock.close()


def main():
    zookeeper_conf_dir="/etc/zookeeper/conf"
    zookeeper_pid_file="/var/run/zookeeper/zookeeper.pid"

    localhost_role=getHostRole()

    if localhost_role == 'follower':
        fl_metrics=prepareFollowerMetrics()
        for metric in fl_metrics:
            message=metrics_prefix+"follower.{} {} {}\n".format(metric, fl_metrics[metric], int(time.time()))
            sendToGraphite(CARBON_SERVER, CARBON_PORT, message)
    elif localhost_role == 'leader':
        ld_metrics=prepareLeaderMetrics()
        fl_metrics=prepareFollowerMetrics()
        for metric in ld_metrics:
            message=metrics_prefix+"leader.{} {} {}\n".format(metric, ld_metrics[metric], int(time.time()))
            sendToGraphite(CARBON_SERVER, CARBON_PORT, message)
        for metric in fl_metrics:
            message=metrics_prefix+"follower.{} {} {}\n".format(metric, fl_metrics[metric], int(time.time()))
            sendToGraphite(CARBON_SERVER, CARBON_PORT, message)


#zkmonitor
if __name__ == "__main__":
    main()
