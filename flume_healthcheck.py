#*/5 * * * * /usr/bin/curl -d "killing" -X POST http://54.179.176.18:8888/kill
#above crontab to issue kill command for the listener at 5mins interval
import SocketServer
from optparse import OptionParser
import socket
from BaseHTTPServer import *
import time
import thread

usage = "usage: %prog [options]"
parser = OptionParser(usage=usage, version="%prog 1.0")

parser.add_option("-l", "--listen_address", action="store", type="string", dest="listen_address", default="",
        help="ip on which to start listening for healthcheck requests [default: \"%default\"]")

parser.add_option("-a", "--listen_port", action="store", type="int", dest="listen_port", default=8888,
        help="port to start listening on [default: \"%default\"]")

(options, args) = parser.parse_args()

def check_flume():
  try:
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      result = s.connect_ex(('127.0.0.1', 81))
      s.close()
      return True
  except:
      return False

class ScribeHC(BaseHTTPRequestHandler):

    def do_GET(self):
        res = check_flume()
        try:
            if res:
                self.send_response(200, "Scribe Server Up!")
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write("OK")
            else:
                self.send_response(503, "Scribe Server Down!")
        except Exception, e:
            if e.errno == errno.EPIPE:
                sys.stderr.write("Looks like the client prematurely closed the connection, ignoring...\n")
            else:
                raise e
    def do_POST(self):
        if self.path.startswith('/kill'):
            print "Server is going down, run it again manually!"
            def kill_me_please(server):
                server.shutdown()
            thread.start_new_thread(kill_me_please, (httpd,))
            self.send_error(500)


class SimpleServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    # Ctrl-C will cleanly kill all spawned threads
    daemon_threads = True
    # much faster rebinding
    allow_reuse_address = True

httpd = SimpleServer((options.listen_address, options.listen_port), ScribeHC)
httpd.serve_forever()
