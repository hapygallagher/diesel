
This Branch
===========
This is a very basic, initial, incomplete expansion on top of Diesel.
WARNING: not fully completed, tested, may even break the standard diesel functionality, would definitely not recommend using this unless feel like contributing / testing, not production ready. Use diesel instead unless you really want the features below:

Very rough description:
- UDPConnection - represents a single port UDP server or a Client "connection", spawns new UDPClientConnections whenever a new connection is made (either as the server when a client connects or when the client successfully connects to th server)
- UDPClientConnection - This represents a conversation between either the Server and a Client or a Client and the server. The UDPConnection spawns one of these when a conversation is started between two endpoints. A UDPClientConnection is spawned whenever a new client starts a UDP conversation with a servers public port. (acts a lot more like the diesel TCP based "Connection") The UDPClientConnection will exist as long as the ConnectedLoop that is spawned when the conversation starts is running. Messages from the associated conversation are automatically sent to the message queue associated with this UDPClientConnection. This allows us to really harness what is useful about using coroutines -- all of the context for the conversation is already in scope, you don't need to look everything up every time you get a message from that client. (which is what is needed for the UDPClient/Socket solution) This cleans up the code a bunch, and abstracts away the details of maintaining the conversation.
- ConnectedLoop - Like a loop, except dedicated to one connection/conversation. creating child loops with "fork_child" will inherit the connection. Main use case is diesel.send will use the connection associated with this ConnectedLoop.


Why Diesel?
===========

You should write your next network application using diesel_.

Thanks to Python_ the syntax is clean and the development pace is rapid. Thanks
to non-blocking I/O it's fast and scalable. Thanks to greenlets_ there's
unwind(to(callbacks(no))). Thanks to nose_ it's trivial to test. Thanks to
Flask_ you don't need to write a new web framework using it.

It provides a clean API for writing network clients and servers. TCP and UDP
supported. It bundles battle-tested clients for HTTP, DNS, Redis, Riak and
MongoDB. It makes writing network applications fun.

Read the documentation, browse the API and join the community in #diesel on
freenode.

Installation
============

Diesel is an active project. Your best bet to stay up with the latest at this
point is to clone from github.::

    git clone git://github.com/jamwt/diesel.git

Once you have a clone, `cd` to the `diesel` directory and install it.::

    pip install .

or::

    python setup.py install

or::

    python setup.py develop


For More Information
====================

Documentation and more can be found on the diesel_ website.


.. _Python: http://www.python.org/
.. _greenlets: http://readthedocs.org/docs/greenlet/en/latest/
.. _nose: http://readthedocs.org/docs/nose/en/latest/
.. _Flask: http://flask.pocoo.org/
.. _diesel: http://diesel.io/
