#!/usr/bin/python2.7
import subprocess
import time
import socket


CARBON_SERVER = 'graphitehost'
CARBON_PORT = 2003
metrics_prefix="prod.aws.app.spark1."

def sendToGraphite(carbon_server, carbon_port, message):
    """
     each message should end with a \n for the send to work
    """
    sock = socket.socket()
    sock.connect( (carbon_server, carbon_port) )
    print "{}".format(message), #, to avoid new line printing
    sock.send(message)
    sock.close()


cmd1=subprocess.Popen(["jps"], stdout=subprocess.PIPE)
cmd2=subprocess.Popen(["grep", "Master"], stdin=cmd1.stdout, stdout=subprocess.PIPE)
cmd1=subprocess.Popen(["jps"], stdout=subprocess.PIPE)
cmd3=subprocess.Popen(["grep", "Worker"], stdin=cmd1.stdout, stdout=subprocess.PIPE)
cmd1=subprocess.Popen(["jps"], stdout=subprocess.PIPE)
cmd4=subprocess.Popen(["grep", "HistoryServer"], stdin=cmd1.stdout, stdout=subprocess.PIPE)
op1=cmd2.communicate()
op2=cmd3.communicate()
op3=cmd4.communicate()
try:
   op1[0].split()[0]
   value_to_send=1
   message=metrics_prefix+"SparkMaster {} {}\n".format(1, int(time.time()))
   sendToGraphite(CARBON_SERVER, CARBON_PORT, message)
except:
   value_to_send=0
   message=metrics_prefix+"SparkMaster {} {}\n".format(0, int(time.time()))
   sendToGraphite(CARBON_SERVER, CARBON_PORT, message)

try: 
   op2[0].split()[0]
   value_to_send=1
   message=metrics_prefix+"SparkWorker {} {}\n".format(1, int(time.time()))
   sendToGraphite(CARBON_SERVER, CARBON_PORT, message)
except:
   value_to_send=0
   message=metrics_prefix+"SparkWorker {} {}\n".format(0, int(time.time()))
   sendToGraphite(CARBON_SERVER, CARBON_PORT, message)

try:
   op3[0].split()[0]
   value_to_send=1
   message=metrics_prefix+"SparkHistoryServer {} {}\n".format(1, int(time.time()))
   sendToGraphite(CARBON_SERVER, CARBON_PORT, message)
except:
   value_to_send=0
   message=metrics_prefix+"SparkHistoryServer {} {}\n".format(0, int(time.time()))
   sendToGraphite(CARBON_SERVER, CARBON_PORT, message)
