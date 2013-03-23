from diesel import UDPConnection
from diesel import log
from diesel import runtime
from diesel.core import UDPClientConnection, Datagram
from diesel.app import UDPConnectionService
from diesel.client import UDPConnectionClient
from struct import Struct
from math import ceil
from collections import deque
from diesel.core import Loop
from diesel import runtime
import diesel.runtime
#import diesel
import sys
import ipdb

class ReliableHeader(object):
    struct = Struct("I i I I")
    multi_seq_struct = Struct("i")
    struct_size = struct.size
    multi_seq_struct_size = multi_seq_struct.size
    #max_msg_len = sys.maxint
    max_msg_len = 576 #lcd max len for udp
    #max_msg_len_multi = Reliable
    max_usr_len = max_msg_len - struct_size
    max_multi_usr_len = max_msg_len - struct_size - multi_seq_struct_size

    max_seq = 65535

#    @staticmethod
#    def max_usr_msg_len(total_length):
#        if total_length > ReliableHeader.max_usr_len:
#            return ReliableHeader.max_multi_usr_len
#        else:
#            return ReliableHeader.max_usr_len

    def __init__(self, seq, msg_len, ack, ack_bits, multi_seq_id = -1):
        self.seq = seq
        self.msg_len = msg_len
        self.ack = ack
        self.ack_bits = ack_bits
        self.multi_seq_id = multi_seq_id

    @staticmethod
    def unpack(packed_data):
        header_size = ReliableHeader.struct_size
        multi_seq_id = -1
        sequence, msg_len, ack, ack_bits = ReliableHeader.struct.unpack(packed_data[0:ReliableHeader.struct_size])
        if (msg_len > ReliableHeader.max_usr_len or msg_len < 0):
            start_idx = ReliableHeader.struct_size
            end_idx = start_idx + ReliableHeader.multi_seq_struct_size
            header_size += ReliableHeader.multi_seq_struct_size
            #ipdb.set_trace()
            multi_seq_id, = ReliableHeader.multi_seq_struct.unpack(packed_data[start_idx:end_idx])
        #log.debug("recd seq: %d, msg_len = %d" % (sequence, msg_len))
        #sequence, ack, ack_bits = ReliableHeader.struct.unpack(packed_data[0:ReliableHeader.struct_size])
        #print("received seq: %d ack: %d ack_bits: 0x%x" % (sequence, ack, ack_bits))
        #return (sequence, ack, ack_bits
        if not hasattr(packed_data, 'addr'):
            ipdb.set_trace()
        dgram = Datagram(str(packed_data[header_size:]), packed_data.addr)
        return (ReliableHeader(sequence, msg_len, ack, ack_bits, multi_seq_id), dgram)

    @staticmethod
    def generate_header(sequence, msg_len, ack, ack_bits, multi_seq_id = -1):
        header = ReliableHeader.struct.pack(sequence, msg_len, ack, ack_bits)
        if multi_seq_id > -1:
            header +=  ReliableHeader.multi_seq_struct.pack(multi_seq_id)
        return header

    def to_data(self):
        return ReliableHeader.generate_header(self.seq, self.msg_len, self.ack, self.ack_bits, self.multi_seq_id)

    @staticmethod
    def calc_num_packets(msg_len):
        return int( ceil(float(msg_len) / float(ReliableHeader.max_multi_usr_len)) )

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

def bit_index_for_sequence(seq, ack):
    assert(seq != ack)
    max_seq = ReliableHeader.max_seq
    assert(not is_seq_more_recent(seq, ack))
    if (seq > ack):
        assert(ack < 33)
        assert(max_seq >= seq)
        return ack + (max_seq - seq)
    else:
        assert(ack >= 1)
        assert(seq <= ack - 1)
        return ack - 1 - seq

class PendingMultiPacketMsg(object):
    def __init__(self, multi_seq_id, msg_seq, msg_len, initial_msg):
        if (msg_len < 0):
            self.first_seq = seq_add(msg_seq, -msg_len) #msg_seq + (-msg_len)
            self.last_seq = -1
            self.num_packets = -1
            self.packets = [None for i in range(-msg_len + 1)]
            self.packets[-msg_len] = initial_msg
            self.multi_seq_id = multi_seq_id
        else:
            self.first_seq = msg_seq
            self.num_packets = ReliableHeader.calc_num_packets(msg_len)
            self.last_seq = seq_add(msg_seq, self.num_packets) #msg_seq + self.num_packets
            self.packets = [None for i in range( self.num_packets)]
            self.packets[0] = initial_msg
            self.multi_seq_id = multi_seq_id
            log.info("NEW PendingMultipacket ID %d, num_packets %d, FIRST: %d, LAST: %d" % (multi_seq_id, self.num_packets, self.first_seq, self.last_seq))

    def _grow_num_packets(self, new_max):
        curr_num_packets = len(self.packets)
        if curr_num_packets < self.num_packets:
            ipdb.set_trace()
            self.packets.join( [None for i in range(self.num_packets - curr_num_packets)])

    def needs_msg(self, multi_seq_id, msg_seq, msg_len, msg):
        if self.num_packets > 0:
            if is_seq_in_range(msg_seq, self.first_seq, self.last_seq) and multi_seq_id == self.multi_seq_id:
                #assert(msg_len < 0) # should only be here if we have already got the first packet!
                try:
                    if self.packets[-msg_len] is None:
                        self.packets[-msg_len] = msg
                except:
                    print self.packets
                    print "msglen %d, len packets %d" % (msg_len, len(self.packets))
                    ipdb.set_trace()
                return True
        else:
            ipdb.set_trace()
            if msg_seq >= 0: # this is the first message
                self.packets[0] = msg
                self.first_seq = msg_seq
                self.num_packets = ReliableHeader.calc_num_packets(msg_len)
                self.multi_seq_id = multi_seq_id
                self._grow_num_packets(self.num_packets)
                self.last_seq = seq_add(self.first_seq, self.num_packets) #self.first_seq + self.num_packets
            else:
                ipdb.set_trace()
                assert(False) #
                self._grow_num_packets(-msg_seq)
                self.packets[-msg_seq]
                self.multi_seq_id = multi_seq_id
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

class FlowControl:

    RTT_THRESHOLD = 250.0

    class Mode:
        Good, Bad = range(2)

    def __init__(self):
        self.mode = FlowControl.Mode.Bad
        self.penalty_time = 4.0
        self.good_conditions_time = 0.0
        self.penalty_reduction_accumulator = 0.0
        self.conn_info = ""

    def send_rate(self):
        if self.mode == FlowControl.Mode.Good:
            return 30.0
        else:
            return 10.0

    def update(self, dt, rtt):
        if self.mode == FlowControl.Mode.Good:
            if rtt > FlowControl.RTT_THRESHOLD:
                log.fields(conn=self.conn_info).info("flow control: dropping to bad mode")
                self.mode = FlowControl.Mode.Bad
                if self.good_conditions_time < 10.0 and self.penalty_time < 60.0:
                    self.penalty_time *= 2.0
                    if self.penalty_time > 60.0:
                        self.penalty_time = 60.0
                    log.fields(conn=self.conn_info).info("flow control: dropping to bad mode")
                    self.good_conditions_time = 0.0
                    self.penalty_reduction_accumulator = 0.0
                    return

            self.good_conditions_time += dt
            self.penalty_reduction_accumulator += dt

            if self.penalty_reduction_accumulator > 10.0 and self.penalty_time > 1.0:
                self.penalty_time /= 2.0
                if self.penalty_time < 1.0:
                    self.penalty_time = 1.0

                log.fields(conn=self.conn_info).info("flow control: penalty time reduced to %f" % self.penalty_time)
                self.penalty_accumulator = 0.0

        elif self.mode == FlowControl.Mode.Bad:
            if rtt <= FlowControl.RTT_THRESHOLD:
                self.good_conditions_time += dt
            else:
                self.good_conditions_time = 0.0

            if self.good_conditions_time > self.penalty_time:
                log.fields(conn=self.conn_info).info("flow control: upgrading to good mode")
                self.good_conditions_time = 0.0
                self.penalty_reduction_accumulator = 0.0
                self.mode = FlowControl.Mode.Good
                return
        else:
            assert(False)



class ReliableUDPClientConnection(UDPClientConnection):

    class ReliabilitySystem(object):

        EPSILON = 0.001
        MAX_ACK_TIME = 0.1
        MISSING_MSG_RESEND_TIME = 1.0

        class PacketData(object):
            def __init__(self, seq, time, data):
                self.seq = seq
                self.time = time
                self.data = data

        class PacketQueue(list):

            def __contains__(self, seq):
                try:
                    for packet in self:
                        if packet.seq == seq:
                            return True
                except:
                    ipdb.set_trace()
                return False

            def insert_sorted(self, packet_data):
                inserted = False
                if len(self) == 0:
                    self.append(packet_data)
                elif is_seq_more_recent(packet_data.seq, self.__getitem__(0).seq):
                    self.insert(0, packet_data)
                elif not is_seq_more_recent(packet_data.seq, self.__getitem__(-1).seq):
                    self.append(packet_data)
                else:
                    for i in range(self.count):
                        if is_seq_more_recent(packet_data.seq, self.__getitem__(i).seq):
                            self.insert(i, packet_data)
                            inserted = True
                        self.append(packet_data)
                    assert(inserted)

            #TODO: validate method

            def __str__(self):
                output = "["
                count = 0
                for item in self:
                    if (count > 0):
                        output += ", "
                    output += str(item.seq)
                    count += 1
                output += "]"
                return output




        def __init__(self, max_seq = ReliableHeader.max_seq):
            self.max_seq = max_seq
            self.local_seq = 0
            self.remote_seq = 0

            self.sent_packets = 0
            self.recv_packets = 0
            self.lost_packets = 0
            self.ackd_packets = 0

            self.sent_bandwidth = 0.0
            self.ackd_bandwidth = 0.0
            self.rtt = 0.0
            self.rtt_max = 1.0 # max permitted RTT

            self.last_msg_dt = 0.0

            self.acks = list()

            self.sentQueue = ReliableUDPClientConnection.ReliabilitySystem.PacketQueue()
            self.pendingAckQueue = ReliableUDPClientConnection.ReliabilitySystem.PacketQueue()
            self.receivedQueue = ReliableUDPClientConnection.ReliabilitySystem.PacketQueue()
            self.ackedQueue = ReliableUDPClientConnection.ReliabilitySystem.PacketQueue()

            self.conn_info = ""

        def packet_sent(self, seq, packet):
            assert(self.local_seq == seq)
            sent_info = ReliableUDPClientConnection.ReliabilitySystem.PacketData(seq, 0.0, packet)
            assert(seq not in self.sentQueue)
            assert(seq not in self.pendingAckQueue)
            self.sentQueue.append(sent_info)
            self.pendingAckQueue.append(sent_info)
            #print "appending to pending Ack Queue seq %d" % sent_info.seq

            self.last_msg_dt = 0.0
            self.local_seq = seq_add(self.local_seq, 1) #self.local_seq + 1
            self.sent_packets += 1

        def packet_received(self, seq, packet):
            if seq in self.receivedQueue:
                log.fields(conn=self.conn_info).warning("received packet that was already received, seq %d" % (seq))
                #ipdb.set_trace()
                return False
            #TODO: not sure if need to include the packet data
            info = ReliableUDPClientConnection.ReliabilitySystem.PacketData(seq, 0.0, None)
            self.receivedQueue.append(info)
            if is_seq_more_recent(seq, self.remote_seq): #remote_msg_seq > self.remote_seq:
                self.remote_seq = seq
            return True

            #self.update()

        def process_ack(self, ack_seq, ack_bits):
            #from net.settings import IS_CLIENT
            #if not IS_CLIENT:
            #    ipdb.set_trace()
            if self.pendingAckQueue:
                log.fields(conn=self.conn_info).info("pre process_ack pending acks: %s" % self.pendingAckQueue)
                new_queue = ReliableUDPClientConnection.ReliabilitySystem.PacketQueue()
                for ack_info in self.pendingAckQueue:
                    acked = False
                    if ack_info.seq == ack_seq:
                        acked = True
                    elif not is_seq_more_recent(ack_info.seq, ack_seq):
                        bit_index = bit_index_for_sequence(ack_info.seq, ack_seq)
                        if (bit_index <= 31):
                            acked = (ack_bits >> bit_index) & 1

                    if acked:
                        log.fields(conn=self.conn_info).info("PROCESSED process_ack for packet %d" % ack_info.seq)
                        self.rtt += (ack_info.time - self.rtt) * 0.1
                        self.ackedQueue.insert_sorted(ack_info)
                        self.acks.append(ack_info.seq)
                        self.ackd_packets += 1
                    else:
                        new_queue.append(ack_info)
                self.pendingAckQueue = new_queue
                log.fields(conn=self.conn_info).info("process_ack pending acks: %s" % self.pendingAckQueue)

        def process_missing_msgs(self):
#            if self.rtt > 0.0:
#                resend_time = self.rtt * 2
#            else:
            #from net.settings import IS_CLIENT
            #if not IS_CLIENT:
            #    ipdb.set_trace()
            resend_time = ReliableUDPClientConnection.ReliabilitySystem.MISSING_MSG_RESEND_TIME
            missed_msgs = []
            new_pending_acks = ReliableUDPClientConnection.ReliabilitySystem.PacketQueue()
            for pending in self.pendingAckQueue:
                if pending.time > resend_time:
                    missed_msgs.append(pending)
                    pending.time = 0.0
                    #ipdb.set_trace()
                else:
                    new_pending_acks.append(pending)
            self.pendingAckQueue = new_pending_acks
            return missed_msgs

        def generate_ack_bits(self, ack):
            ack_bits = 0
            for recvd in self.receivedQueue:
                if recvd.seq == ack or is_seq_more_recent(recvd.seq, ack):
                    break
                bit_index = bit_index_for_sequence(recvd.seq, ack)
                if bit_index <= 31:
                    ack_bits |= 1 << bit_index
            return ack_bits

        def advance_queue_time(self, dt):
            for info in self.sentQueue:
                info.time += dt
            for info in self.pendingAckQueue:
                info.time += dt
            for info in self.receivedQueue:
                info.time += dt
            for info in self.ackedQueue:
                info.time += dt

        def update_queues(self):
            while self.sentQueue and (self.sentQueue[0].time > (self.rtt_max + self.EPSILON) ):
                sent = self.sentQueue.pop(0)
                log.fields(conn=self.conn_info).info("SENTQUEUE: removing seq %d" % sent.seq)

            if self.receivedQueue:
                last_seq = self.receivedQueue[-1].seq
                if last_seq >= 34:
                    min_seq = last_seq - 34
                else:
                    min_seq = self.max_seq - (34 - last_seq)

                while self.receivedQueue and not is_seq_more_recent(self.receivedQueue[-1].seq, min_seq):
                    recd = self.receivedQueue.pop(0)
                    log.fields(conn=self.conn_info).info("RECDQUEUE: removing seq %d" % recd.seq)

            while self.ackedQueue and self.ackedQueue[-1].time > (self.rtt_max * 2 - self.EPSILON):
                ackd = self.ackedQueue.pop(0)
                log.fields(conn=self.conn_info).info("ACKDQUEUE: removing seq %d" % ackd.seq)

            while self.pendingAckQueue and self.pendingAckQueue[-1].time > (self.rtt_max + self.EPSILON):
                packd = self.pendingAckQueue.pop(0)
                log.fields(conn=self.conn_info).info("PACKDQUEUE: removing seq %d" % packd.seq)
                self.lost_packets += 1

        def update_stats(self):
            #TODO: don't use len(data) to figure out how much data was sent
            sent_bytes_per_second = 0

            for sent in self.sentQueue:
                sent_bytes_per_second += len(sent.data)

            acked_packets_per_second = 0
            acked_bytes_per_second = 0

            for ackd in self.ackedQueue:
                if ackd.time >= self.rtt_max:
                    acked_packets_per_second += 1
                    acked_bytes_per_second += len(ackd.data)

            sent_bytes_per_second /= self.rtt_max
            acked_bytes_per_second /= self.rtt_max
            self.sent_bandwidth = sent_bytes_per_second * ( 8. / 1000.0 )
            self.ackd_bandwidth = acked_bytes_per_second * ( 8. / 1000.0 )

        def update(self, dt):
            self.acks = []
            self.advance_queue_time(dt)
            self.update_queues()
            self.update_stats()
            self.last_msg_dt += dt
            #TODO: if __debug__ ? self.validate()

    def __init__(self, parent, sock, remote_addr):
        super(ReliableUDPClientConnection, self).__init__(parent, sock, remote_addr)
        self.reliability = ReliableUDPClientConnection.ReliabilitySystem()
        self.flow_control = ReliableUDPClientConnection.FlowControl()
        self.conn_info = "(%s:%s)" % (self.addr, self.port)
        self.reliability.conn_info = self.conn_info
        self.flow_control.conn_info = self.conn_info
        #self.local_seq = 0
        #self.remote_seq = 0
        #self.sent_queue = list()
        #self.recv_queue = list()
        #self.ack_queue = list()
        #self.pending_ack_queue = list()
        self.pending_multipackets = deque()
        self.multi_packet_seq = 0

    def _internal_queue_outgoing(self, outgoing_dgram, priority):
        self.last_msg_dt = 0.0
        self.reliability.packet_sent(self.reliability.local_seq, outgoing_dgram)
        super(ReliableUDPClientConnection, self).queue_outgoing(outgoing_dgram, priority)


    def update(self, dt):
        self.reliability.update(dt)
        self.flow_control.update(dt, self.reliability.rtt * 1000.0)
        missing_msgs = self.reliability.process_missing_msgs()
        for msg in missing_msgs:
            log.fields(conn=self.conn_info).info("resending unacked msg, seq: {0}", msg.seq)
            #ipdb.set_trace()
            data_dgram = Datagram(msg.data, self.addr)
            header, msgdata = ReliableHeader.unpack(data_dgram)
            header.seq = self.reliability.local_seq
            dgram = header.to_data() + msgdata
            self._internal_queue_outgoing(dgram, 1)
        if self.reliability.last_msg_dt > ReliableUDPClientConnection.ReliabilitySystem.MAX_ACK_TIME:
            self.queue_outgoing("")

    def process_datagram(self, dgram):
        #remote_msg_seq, msg_len, dgram = ReliableHeader.unpack(dgram)
        header, dgram = ReliableHeader.unpack(dgram)
        remote_msg_seq = header.seq
        msg_len = header.msg_len
        multi_seq_id = header.multi_seq_id
        log.fields(conn=self.conn_info).info("recvd dgram : remote_seq %d, msg_len %d" % (remote_msg_seq, msg_len))

        self.reliability.packet_received(header.seq, dgram)
        if ((self.addr, self.port) != (dgram.addr)):
            ipdb.set_trace()
        log.fields(conn=self.conn_info).info("processing ack for (%s, %s)" % (self.addr, self.port))
        self.reliability.process_ack(header.ack, header.ack_bits)

        if (msg_len > ReliableHeader.max_usr_len or msg_len < 0):
            log.fields(conn=self.conn_info).debug("processing as multipacket")
            processed_msg = False
            for pending in self.pending_multipackets:
                if pending.needs_msg(multi_seq_id, remote_msg_seq, msg_len, dgram):
                    processed_msg = True
                    log.fields(conn=self.conn_info).debug("adding to pending multipackets")
                    if pending.is_complete():
                        #ipdb.set_trace()
                        complete_msg = pending.get_msg()
                        super(ReliableUDPClientConnection, self).process_datagram(complete_msg)
                        self.pending_multipackets.remove(pending)
                        break
            if not processed_msg:
                log.fields(conn=self.conn_info).debug("new pending multipackets")
                pending_multipacket = PendingMultiPacketMsg(multi_seq_id, remote_msg_seq, msg_len, dgram)
                self.pending_multipackets.append(pending_multipacket)
        else:
            if len(dgram) > 0: # if length zero then it was purely an ack packet
                super(ReliableUDPClientConnection, self).process_datagram(dgram)

    def queue_outgoing(self, msg, priority=5, reliable_header = None):
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
        multi_packet_seq = -1
        max_msg_bytes = ReliableHeader.max_usr_len
        if total_bytes > ReliableHeader.max_usr_len:
            multi_packet_seq = self.multi_packet_seq
            self.multi_packet_seq = seq_add(self.multi_packet_seq, 1)
            max_msg_bytes = ReliableHeader.max_multi_usr_len

        while True: #do while curr_end_idx < total_bytes:

            curr_len = total_bytes - curr_end_idx
            if (curr_len > max_msg_bytes):
                curr_len = max_msg_bytes
            log.fields(conn=self.conn_info).debug("curr len %d" % curr_len)

            curr_end_idx = curr_start_idx + curr_len

            msg_bytes = dgram[curr_start_idx:curr_end_idx]

            if packet_count > 0:
                msg_len_field = -packet_count
            else:
                msg_len_field = total_bytes

            curr_packet_seq = self.reliability.local_seq
            log.fields(conn=self.conn_info).debug("gen header local_seq %d msg_len %d" % (self.reliability.local_seq, msg_len_field))
            ack_seq = self.reliability.remote_seq
            header_bytes = ReliableHeader.generate_header(self.reliability.local_seq, msg_len_field, ack_seq, self.reliability.generate_ack_bits(ack_seq), multi_packet_seq)
            #self.reliability.local_seq = seq_add(self.reliability.local_seq, 1) #self.local_seq + 1
            assert(len(header_bytes + msg_bytes) <= ReliableHeader.max_msg_len)
            outgoing_dgram = Datagram(header_bytes + msg_bytes, dgram.addr)
            #log.fields(conn=self.conn_info).debug("dgram: %s", outgoing_dgram)

            curr_start_idx = curr_end_idx
            packet_count = packet_count + 1

            #self.reliability.packet_sent(curr_packet_seq, outgoing_dgram)
            #super(ReliableUDPClientConnection, self).queue_outgoing(outgoing_dgram, priority)

            self._internal_queue_outgoing(outgoing_dgram, priority)

            #DO WHILE
            if curr_end_idx >= total_bytes:
                break



class ReliableUDPConnection(UDPConnection):

    MAX_TIME_BETWEEN_ACKS = 0.5

    def __init__(self, parent, sock, ip=None, port=None, f_connection_loop = None, *args, **kw ):
        super(ReliableUDPConnection, self).__init__(parent, sock, ip, port, f_connection_loop, *args, **kw)
        l = Loop(self.update_client_connections)
        runtime.current_app.add_loop(l)
        #diesel.fork_child(self.update_client_connections)

    def update_client_connections(self):
        import time
        prev_time = time.clock()
        while True:
            #TODO: debug timing slower for now
            diesel.sleep(ReliableUDPConnection.MAX_TIME_BETWEEN_ACKS)   #TODO: not sure about the timing
            curr_time = time.clock()
            dt = curr_time - prev_time
            prev_time = curr_time
            for sub_connection in self.udp_connections.itervalues():
                sub_connection.update(dt)

    def _create_new_connection(self, sock, remote_addr):
        log.debug("RELIABLEUDPCONNECION: create_new_conenection")
        return ReliableUDPClientConnection(self, sock, remote_addr)

class ReliableUDPConnectionService(UDPConnectionService):
    def _create_new_connection(self, parent, sock, ip, port, f_connection_loop, *args, **kw):
        log.debug("RELIABLEUDPCONNECION SERVICE: create_new_conenection")
        return ReliableUDPConnection(parent, sock, ip, port, f_connection_loop, *args, **kw)

class ReliableUDPConnectionClient(UDPConnectionClient):
    def _create_new_connection(self, parent, sock, ip, port, f_connection_loop, *args, **kw):
        log.debug("RELIABLEUDP CLIENT CONNECION: create_new_conenection")
        return ReliableUDPConnection(parent, sock, ip, port, f_connection_loop, *args, **kw)


