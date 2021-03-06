#!/usr/bin/env python
import select
import socket
import struct
import sys
import threading
import time

PROTOCOLVERSION = b'\01'
CLNT_BCAST_EX = b'\02'
CLNT_UCAST_EX = b'\03'
CLNT_UCAST_INST = b'\04'
CLNT_UCAST_DAC = b'\0F'

SVR_RESP = b'\05'


class MCSQLRServer(object):

    def __init__(self):

        switch = {CLNT_BCAST_EX: ['CLNT_BCAST_EX', self._svr_resp],
                  CLNT_UCAST_EX: ['CLNT_UCAST_EX', self._svr_resp],
                  CLNT_UCAST_INST: ['CLNT_UCAST_INST', self._svr_resp],
                  CLNT_UCAST_DAC: ['CLNT_UCAST_DAC', self._svr_resp_dac]}

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(('', 1434))

        self._sTime = time.strftime('%Y-%m-%d %H:%m:%S ')
        print(self._sTime + 'WAITING FOR REQUEST FROM CLIENT')

        while True:
            data, address = self._sock.recvfrom(1024)
            if data:
                self._sTime = time.strftime('%Y-%m-%d %H:%m:%S ')
                req_type = data[0:1]
                if req_type not in switch:
                    print(self._sTime + 'Unknown request from ' + address[0])
                    return

                print(self._sTime + 'Received ' + switch[req_type][0] +
                      ' from ' + address[0])
                req_opt = [address]
                switch[req_type][1](*req_opt)

    def _svr_resp(self, address):
        # TODO: 2.2.5: resp_data MUST be of size:
        #   CLNT_UCAST_INST - 1024
        #   CLNT_BCAST/UCAST_EX - 65535
        resp_data = 'ServerName;ILSUNG1;InstanceName;YUKON;IsClustered;No;Version;9.00.1399.06;tcp;57137;;ServerName;ILSUNG1;InstanceName;YUKONDEV;IsClustered;No;Version;9.00.1399.06;np;\\\\ILSUNG1\\pipe\\MSSQL$YUKONDEV\\sql\\query;;ServerName;ILSUNG1;InstanceName;MSSQLSERVER;IsClustered;No;Version;9.00.1399.06;tcp;1433;np;\\\\ILSUNG1\\pipe\\sql1\\query;;'
        resp_size = struct.pack('<H', len(resp_data))
        self._sock.sendto(SVR_RESP + resp_size + resp_data, address)

    def _svr_resp_dac(self):
        print("* Not supported")
        pass


class ServerResponse(object):
    class Instance(object):

        def __init__(self, inst_data):
            """ Build an SSRP Server Instance given raw data or elements. """
            self._attributes = {}
            self._inst_data = inst_data

            # TODO: Split this off to its own internal function.
            # Add opposite of __getitem__ so we can manually build an
            # instance... figure out where this and the ServerResponse
            # goes in relation to both the client and server.

            token_nvpairs = self._inst_data.split(';')
            # Each Token Name/Value pair is separated by a single colon.
            for i in range(0, len(token_nvpairs), 2):
                # TODO?: I'm afraid this is pretty loose.
                # Section 2.2.5 SVR_RESP seems to indicate that some tokens
                # may contain additional parameters like so: name;value;params,
                # but this section seems kind of ambiguious and stops short at
                # actually spelling out what a server response should be. I wish
                # there was a comprehensive list of valid Token and example
                # data. I haven't encountered a problem with this simplified
                # approach, but it's not unlikely to be wrong.
                self._attributes[token_nvpairs[i]] = token_nvpairs[i+1]

        def __getitem__(self, s):
            """ Returns instance attribute or None if key is not found. """
            ret = None
            if s in self._attributes:
                ret = self._attributes[s]
            return ret

    def __init__(self, resp_type=None):

        # TODO: There is a lot of additional verification we could be
        # doing per type of response expected.

        self.isvalid = False
        self.iscomplete = False

        self._address = None
        self._resp_data = None
        self._resp_size = None
        self._resp_type = resp_type

    def append(self, recv_str):
        try:
            """Append receive and mark .isvalid/.iscomplete accordingly."""
            if self._resp_data is None:
                if not recv_str[0:1] == SVR_RESP:
                    return
                self.isvalid = True
                self._resp_size = struct.unpack('<H', recv_str[1:3])[0]
                self._resp_data = recv_str[3:]

            else:
                self._resp_data += recv_str

            if len(self._resp_data) == self._resp_size:
                self.iscomplete = True
                self._instances = self.get_instances()

            if len(self._resp_data) > self._resp_size:
                self.isvalid = False

        except Exception as e:
            print("An error occurred in ServerResponse.append():\n" + str(e))

    def get_instances(self):
        """
        This will be expanded to handle separating elements of a
        response into individually accessible variables.
        """
        instances = []
        # Instances are separated by a double semicolon. The full message
        # also happens to end in a double semicolon which we don't care to see.
        raw_instances = self._resp_data[:-2].split(';;')
        for rinst in raw_instances:
            instances.append(self.Instance(rinst))

        return instances

    def __len__(self):
        return len(self._instances)


class MCSQLRClient(object):

    def __init__(self, req_type, req_opt=[], callback=None):
        """Initialize MCSQLRClient.

        Arguments:
        request_type     - The type of request to perform (i.e. CLNT_*)
        request_options  - Tuple with additional options for type of request
        callback         - Callback function for each valid response received
        """
        self._req_type = req_type
        self._server_responses = []

        switch = {CLNT_BCAST_EX: self._clnt_bcast_ex,
                  CLNT_UCAST_EX: self._clnt_ucast_ex,
                  CLNT_UCAST_INST: self._clnt_ucast_ex,
                  CLNT_UCAST_DAC: self._clnt_ucast_dac}

        switch[req_type](*req_opt)
        self._client.bind(('', 0))
        self._readers = [self._client]
        self._writers = [self._client]
        self._reader_callback = callback
        self._quit = False

        thread = threading.Thread(target=self._cycle)
        thread.start()

    def _clnt_bcast_ex(self):
        """Setup request to identify database instances on a network."""

        self._client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        # TODO: This (<broadcast>) isn't going to work for IPV6
        self._address = ('<broadcast>', 1434)
        self._wbuff = CLNT_BCAST_EX

    def _clnt_ucast_ex(self, req_options):
        """Request to identify DB instances on a server.

        Keyword argumennts:
        address -- tuple containing address and port to query
        """
        self._address = address
        self._client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._wbuff = CLNT_UCAST_EX

    def _clnt_ucast_inst(self, instancename):
        """Request for details of a particular instance on a server.

        Keyword arguments:
        address -- tuple containing address and port to query
        instance -- string name of instance to query
        """
        self._address = address
        self._client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._wbuff = CLNT_UCAST_INST + instancename

    def _clnt_ucast_dac(self):
        # TODO: Complete me if necessary...
        pass

    def _cycle(self):
        try:
            self._client.settimeout(1)
            while self._quit is False:
                rlist, wlist, xlist = select.select(
                    self._readers, self._writers, [], 1)
                for reader in rlist:
                    self._recvdata()
                for writer in wlist:
                    self._senddata(writer)

        except Exception as e:
            print("An error occurred in Client.cycle()\n" + str(e))

    def _senddata(self, writer):
        """ Send data in the write buffer without any verification. """
        try:
            sentcount = 0
            bufferlen = len(self._wbuff)
            while sentcount < bufferlen:
                sent = self._client.sendto(self._wbuff[sentcount:], self._address)
                if sent == 0:
                    raise RuntimeError("socket connection broken")
                sentcount += sent
                if sentcount == bufferlen:
                    self._wbuff = b''
                    self._writers.remove(writer)
        except Exception as e:
            print("An error occurred in Client._senddata()\n" + str(e))

    def _recvdata(self):
        """Receive responses from a SSRP broadcast request."""
        try:
            resp_data = ''
            server_response = ServerResponse(self._req_type)
            while True:
                # TODO: Limit the bufsize based on _req_type?
                recv_str, address = self._client.recvfrom(65538)
                if recv_str == '':
                    raise RuntimeError("socket connection broken")
                server_response.append(recv_str)
                if not server_response.isvalid:
                    # TODO: 3.2.5.4 Waiting Completed
                    # Only CLNT_BCAST_EX ignores invalid messages
                    # if not self._req_type == CLNT_BCAST_EX:
                    #   raise SomeError(...)
                    break
                if server_response.iscomplete:
                    if self._reader_callback is not None:
                        # This is actually a violation of the RFC.
                        # 3.2.5.4 Waiting Completed states a client SHOULD
                        # buffer all responses until timer has timed out.
                        self._server_responses.append(server_response)
                        self._reader_callback()
                    break

        except Exception as e:
            print('An error occurred in Client._recvdata():\n' + str(e))

    def close(self):
        """Stop _cycle() and close our socket client."""
        try:
            self._quit = True
            self._client.close()
        except Exception as e:
            print("An error occurred in Client.close():\n" + str(e))


def callback():
    # TODO: Iteration
    pass
#    print(len(client._server_responses))

if __name__ == "__main__":

    if '--server' in sys.argv:
        server = MCSQLRServer()

    else:

        # Create client and listen for incoming responses
        client = MCSQLRClient(
            req_type=CLNT_BCAST_EX,
            req_opt=[],
            callback=callback
        )

        # Wait for a timeout period (in case of slow resp) before closing
        time.sleep(2)
        client.close()

        # TODO: see callback, put this there
        for response in client._server_responses:
            print("\n\tServer Name: {0} (contains {1} instances)".format(
                 response._instances[0]['ServerName'], len(response)))
            for instance in response._instances:
                print("\tInstance: \t{0}".format(instance['InstanceName']))
                print("\t   Version: \t{0}".format(instance['Version']))
                print("\t   IsClustered: {0}".format(instance['IsClustered']))
                print("\t   TCP Port: \t{0}".format(instance['tcp']))

