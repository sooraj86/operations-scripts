#!/usr/bin/python2.7
import subprocess
import time
import socket

CARBON_SERVER = 'graphitehost'
CARBON_PORT = 2003
metrics_prefix="prod.aws.app.kafka1."

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
cmd2=subprocess.Popen(["grep", "Kafka"], stdin=cmd1.stdout, stdout=subprocess.PIPE)
try:
   op=cmd2.communicate()
except:
   message=metrics_prefix+"kafkaservice {} {}\n".format(0, int(time.time()))
   sendToGraphite(CARBON_SERVER, CARBON_PORT, message)

if op[0].split()[0]:
   message=metrics_prefix+"kafkaservice {} {}\n".format(1, int(time.time()))
   sendToGraphite(CARBON_SERVER, CARBON_PORT, message)
else:
   message=metrics_prefix+"kafkaservice {} {}\n".format(0, int(time.time()))
   sendToGraphite(CARBON_SERVER, CARBON_PORT, message)


