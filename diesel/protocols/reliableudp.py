from diesel import UDPConnection
from diesel.core import UDPClientConnection, Datagram
from diesel.app import UDPConnectionService
from diesel.client import UDPConnectionClient
from struct import Struct
from math import ceil
from collections import deque
import sys
import ipdb

class ReliableHeader(object):
    struct = Struct("I i")
    struct_size = struct.size
    max_msg_len = sys.maxint
    max_msg_len = 576 #lcd max len for udp
    max_usr_len = max_msg_len - struct_size
    max_seq = 65535

    @staticmethod
    def unpack(packed_data):
        sequence, msg_len = ReliableHeader.struct.unpack(packed_data[0:ReliableHeader.struct_size])
        print("recd seq: %d, msg_len = %d" % (sequence, msg_len))
        #sequence, ack, ack_bits = ReliableHeader.struct.unpack(packed_data[0:ReliableHeader.struct_size])
        #print("received seq: %d ack: %d ack_bits: 0x%x" % (sequence, ack, ack_bits))
        #return (sequence, ack, ack_bits)
        dgram = Datagram(str(packed_data[ReliableHeader.struct_size:]), packed_data.addr)
        return (sequence, msg_len, dgram)

    @staticmethod
    def generate_header(sequence, msg_len):
        return ReliableHeader.struct.pack(sequence, msg_len)

    @staticmethod
    def calc_num_packets(msg_len):
        return int( ceil(float(msg_len) / float(ReliableHeader.max_usr_len)) )

#class PacketData(object):
#    def __init__(self, sequence, time, size):
#        self.sequence = sequence
#        self.time = time
#        self.size = size

def is_seq_more_recent(seq1, seq2):
    max_seq = ReliableHeader.max_seq
    return (((seq1 > seq2) and (seq1 - seq2 <= max_seq/2)) or ((seq2 > seq1) and (seq2 - seq1 > max_seq/2)))

def is_seq_in_range(test, min_test, max_test):
    max_seq = ReliableHeader.max_seq
    if (min_test > max_seq/2) and (max_test < max_seq/2):
        return (test >= min_test and test <= max_seq) or ( test <= max_test )
    else:
        return test >= min_test and test <= max_test

def seq_add(seq, amount):
    seq = seq + amount
    max_seq = ReliableHeader.max_seq
    if seq < 0:
        seq += max_seq
    if seq > ReliableHeader.max_seq:
        seq -= max_seq
    return seq

class PendingMultiPacketMsg(object):
    def __init__(self, msg_seq, msg_len, initial_msg):
        if (msg_len < 0):
            self.first_seq = seq_add(msg_seq, -msg_len) #msg_seq + (-msg_len)
            self.last_seq = -1
            self.num_packets = -1
            self.packets = [None for i in range(-msg_len + 1)]
            self.packets[-msg_len] = initial_msg
        else:
            self.first_seq = msg_seq
            self.num_packets = ReliableHeader.calc_num_packets(msg_len)
            self.last_seq = seq_add(msg_seq, self.num_packets) #msg_seq + self.num_packets
            self.packets = [None for i in range( self.num_packets)]
            self.packets[0] = initial_msg

    def _grow_num_packets(self, new_max):
        curr_num_packets = len(self.packets)
        if curr_num_packets < self.num_packets:
            self.packets.join( [None for i in range(self.num_packets - curr_num_packets)])

    def needs_msg(self, msg_seq, msg_len, msg):
        if self.num_packets > 0:
            if is_seq_in_range(msg_seq, self.first_seq, self.last_seq):
                assert(msg_len < 0) # should only be here if we have already got the first packet!
                if self.packets[-msg_len] is None:
                    self.packets[-msg_len] = msg
                return True
        else:
            if msg_seq >= 0: # this is the first message
                self.packets[0] = msg
                self.first_seq = msg_seq
                self.num_packets = ReliableHeader.calc_num_packets(msg_len)
                self._grow_num_packets(self.num_packets)
                self.last_seq = seq_add(self.first_seq, self.num_packets) #self.first_seq + self.num_packets
            else:
                self._grow_num_packets(-msg_seq)
                self.packets[-msg_seq]
        return False

    def is_complete(self):
        if (self.num_packets < 0):
            return False
        for packet in self.packets:
            if packet is None:
                return False
        return True

    def get_msg(self):
        msg = ""
        for packet in self.packets:
            assert(packet is not None)
            msg += packet
        return msg


class ReliableUDPClientConnection(UDPClientConnection):
    def __init__(self, parent, sock, remote_addr):
        super(ReliableUDPClientConnection, self).__init__(parent, sock, remote_addr)
        self.local_seq = 0
        self.remote_seq = 0
        #self.sent_queue = list()
        #self.recv_queue = list()
        #self.ack_queue = list()
        #self.pending_ack_queue = list()
        self.pending_multipackets = deque()

    def process_datagram(self, dgram):
        remote_msg_seq, msg_len, dgram = ReliableHeader.unpack(dgram)
        print("recvd dgram : remote_seq %d, msg_len %d" % (remote_msg_seq, msg_len))
        if is_seq_more_recent(remote_msg_seq, self.remote_seq): #remote_msg_seq > self.remote_seq:
            self.remote_seq = remote_msg_seq

        if (msg_len > ReliableHeader.max_usr_len or msg_len < 0):
            processed_msg = False
            for pending in self.pending_multipackets:
                if pending.needs_msg(remote_msg_seq, msg_len, dgram):
                    processed_msg = True
                    if pending.is_complete():
                        complete_msg = pending.get_msg()
                        super(ReliableUDPClientConnection, self).process_datagram(complete_msg)
                        self.pending_multipackets.remove(pending)
                        break
            if not processed_msg:
                pending_multipacket = PendingMultiPacketMsg(remote_msg_seq, msg_len, dgram)
                self.pending_multipackets.append(pending_multipacket)
        else:
            super(ReliableUDPClientConnection, self).process_datagram(dgram)

    def queue_outgoing(self, msg, priority=5):
        if isinstance(msg, Datagram):
            dgram = msg
        else:
            dgram = Datagram(msg, (self.addr, self.port))

        #break our datagram into multiple if larger than the allowed size
        total_bytes = len(dgram) #TODO: check this is getting the right length
        #assert(total_bytes <= ReliableHeader.max_msg_len)
        curr_start_idx = 0
        curr_end_idx = 0
        curr_len = 0
        packet_count = 0
        while curr_end_idx < total_bytes:

            curr_len = total_bytes - curr_end_idx
            if (curr_len > ReliableHeader.max_usr_len):
                curr_len = ReliableHeader.max_usr_len
            print("curr len %d" % curr_len)

            curr_end_idx = curr_start_idx + curr_len

            msg_bytes = dgram[curr_start_idx:curr_end_idx]

            if packet_count > 0:
                msg_len_field = -packet_count
            else:
                msg_len_field = total_bytes

            print("gen header local_seq %d msg_len %d" % (self.local_seq, msg_len_field))
            header_bytes = ReliableHeader.generate_header(self.local_seq, msg_len_field)
            self.local_seq = seq_add(self.local_seq, 1) #self.local_seq + 1
            assert(len(header_bytes + msg_bytes) <= ReliableHeader.max_msg_len)
            outgoing_dgram = Datagram(header_bytes + msg_bytes, dgram.addr)
            super(ReliableUDPClientConnection, self).queue_outgoing(outgoing_dgram, priority)

            curr_start_idx = curr_end_idx
            packet_count = packet_count + 1

class ReliableUDPConnection(UDPConnection):
    def _create_new_connection(self, sock, remote_addr):
        return ReliableUDPClientConnection(self, sock, remote_addr)

class ReliableUDPConnectionService(UDPConnectionService):
    def _create_new_connection(self, parent, sock, ip, port, f_connection_loop, *args, **kw):
        return ReliableUDPConnection(parent, sock, ip, port, f_connection_loop, *args, **kw)

class ReliableUDPConnectionClient(UDPConnectionClient):
    def _create_new_connection(self, parent, sock, ip, port, f_connection_loop, *args, **kw):
        return ReliableUDPConnection(parent, sock, ip, port, f_connection_loop, *args, **kw)


