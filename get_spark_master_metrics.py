import json
import time
import socket
import requests

CARBON_SERVER = 'graphitehost'
CARBON_PORT = 2003
metrics_prefix="prod.aws.app.spark1."
spark_master='sparkmasterhost'

def sendToGraphite(carbon_server, carbon_port, message):
    """
     each message should end with a \n for the send to work
    """
    sock = socket.socket()
    sock.connect( (carbon_server, carbon_port) )
    print "{}".format(message), #, to avoid new line printing
    sock.send(message)
    sock.close()

master_metrics_request=requests.get("http://"+spark_master+":8080/metrics/master/json")
master_metrics=json.loads(master_metrics_request.text)
for keys in master_metrics['gauges']:
    message=metrics_prefix+keys+" {} {}\n".format(master_metrics['gauges'][keys]['value'], int(time.time()))
    sendToGraphite(CARBON_SERVER, CARBON_PORT, message)

