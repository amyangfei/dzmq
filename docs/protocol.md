* client: message from client to dzbroker **localfe**
	1. original message
    	* Frame 0: data
	2. client call mdp_client_send() with a **service** name
    	* Frame 1: empty frame (delimiter)
    	* Frame 2: "MDPCxy" (six bytes, MDP/Client x.y)
    	* Frame 3: service name (printable string)
    	* Frame 4: data

* client: receive message back from dzbroker's **localfe**
    1. receive message back from localfe
        * Frame 1: empty
        * Frame 2: header (MDPC_CLIENT)
        * Frame 3: command (REPORT|NAK) //TODO: NAK suppoprt
        * Frame 4: service name
        * Frame 5: data

* debroker: dzbroker **localfe** receive client's request and then call s_broker_client_msg() with from_local=true
	1. receive message
		* Frame 1: client address
		* Frame 2: empty
		* Frame 3: header
		* Frame 4: service name
		* Frame 5: data
	2. if self->local_capacity > 0, send to **localbe**
		* Frame 1: worker address
		* Frame 2: empty
		* Frame 3: header (MDPW_WORKER)
		* Frame 4: command (MDPW_REQUEST)
		* Frame 5: service name
		* Frame 6: client address
		* Frame 7: empty
		* Frame 8: data
	3. else self->local_capacity == 0, send to **cloudbe**
		* Frame 1: peer broker's cloudfe address
		* Frame 2: header (MDPC_CLIENT)
		* Frame 3: command (MDPC_REPOST)
		* Frame 4: service name
		* Frame 5: self->cloudbe address
		* Frame 6: client address
		* Frame 7: empty
		* Frame 8: data

* dzbroker: dzbroker **cloudfe** receive request routered by peer broker's cloudbe and then call s_broker_client_msg() with from_local=false
	1. receive message routered by peer broker's cloudbe
		* Frame 1: whose address?
        * Frame 2: header (MDPC_CLIENT)
        * Frame 3: command (MDPC_REPOST)
        * Frame 4: service name
		* Frame 5: cloudbe address
        * Frame 6: client address
		* Frame 7: empty
		* Frame 8: data
	2. will only send message to **localbe**
        * Frame 1: worker address
		* Frame 2: empty
		* Frame 3: header (MDPW_WORKER)
		* Frame 4: command (MDPC_REPOST)
		* Frame 5: service name
		* Frame 6: peer's cloudbe address
		* Frame 7: client address
		* Frame 8 : empty
		* Frame 9: data

* worker: worker receive from **localbe** by mdp_worker_recv()
    1. receive message from localbe with command **MDPW_REQUEST**
		* Frame 1: empty
		* Frame 2: header (MDPW_WORKER)
		* Frame 3: command (MDPW_REQUEST)
		* Frame 4: service name
		* Frame 5: client address
		* Frame 6: empty
		* Frame 7: data
    2. receive message from localbe with command **MDPW_REPOST**
		* Frame 1: empty
		* Frame 2: header (MDPW_WORKER)
		* Frame 3: command (MDPW_REPOST)
		* Frame 4: service name
		* Frame 5: peer's cloudbe address
		* Frame 6: client address
		* Frame 7: empty
		* Frame 8: data
    3. scene 1: message after analyse (client's address has been stored in worker_t's reply_to)
        * Frame 1: command (MDPW_REPORT_LOCAL)
		* Frame 2: service name
		* Frame 3: data
    4. scene 2: message after analyse (client's address has been stored in worker_t's reply_to)
        * Frame 1: command (MDPW_REPORT_CLOUD)
		* Frame 2: peer's cloudbe address
		* Frame 3: service name
		* Frame 4: data

* worker: worker does something with message recievied and then send back by mdp_worker_send()
    1. original message scene 1
        * Frame 1: command (MDPW_REPORT_LOCAL)
		* Frame 2: service name
		* Frame 3: data
    2. original message scene 2
        * Frame 1: command (MDPW_REPORT_CLOUD)
		* Frame 2: peer's cloudbe address
		* Frame 3: service name
		* Frame 4: data
    3. handle original message, envelop it and send back scene 1
        * Frame 1: empty
		* Frame 2: header (MDPW_WORKER)
        * Frame 3: command (MDPW_REPORT_LOCAL)
        * Frame 4: client address
        * Frame 5: empty
		* Frame 6: service name
		* Frame 7: data
    4. handle original message, envelop it and send back scene 2
        * Frame 1: empty
		* Frame 2: header (MDPW_WORKER)
        * Frame 3: command (MDPW_REPORT_LOCAL)
        * Frame 4: client address
        * Frame 5: empty
		* Frame 6: peer's cloudbe address
		* Frame 7: service name
		* Frame 8: data
    5. worker connect or reconnect to broker by calling s_mdp_worker_connect_to_broker()
        * Frame 1: empty
        * Frame 2: header (MDPW_WORKER)
        * Frame 3: command (MDPW_READY)
    6. TODO
        * HEARTBEAT
        * DISCONNECT

* dzbroker: dzbroker receive from **localbe**, message from one of dzbroker's worker
    1. worker READY message
        * Frame 1: worker address
        * Frame 2: empty
        * Frame 3: header (MDPW_WORKER)
        * Frame 4: command (MDPW_READY)
    2. message with command **MDPW_REPORT_LOCAL**, so worker has dealed with **local** client's request
        * Frame 1: worker address
        * Frame 2: empty
        * Frame 3: header (MDPW_WORKER)
        * Frame 4: command (MDPW_REPORT_LOCAL)
        * Frame 5: client address
        * Frame 6: empty
		* Frame 7: service name
		* Frame 8: data
    3. message with command **MDPW_REPORT_CLOUD**, so worker has dealed with **peer** client's request
        * Frame 1: worker address
        * Frame 2: empty
        * Frame 3: header (MDPW_WORKER)
        * Frame 4: command (MDPW_REPORT_CLOUD)
        * Frame 5: client address
        * Frame 6: empty
		* Frame 7: peer's cloudbe address
		* Frame 8: service name
		* Frame 9: data
    4. scene 2: send message back to **localfe**, then it will be routered to local client
        * Frame 1: client address
        * Frame 2: empty
        * Frame 3: header (MDPC_CLIENT)
        * Frame 4: command (MDPC_REPORT)
		* Frame 5: service name
		* Frame 6: data
    5. scene 3: send message to self's **cloudfe** then it will be routered to peer's **cloudbe** with address equals to frame1
		* Frame 1: peer's cloudbe address
        * Frame 2: client address
        * Frame 3: empty
        * Frame 4: header (MDPW_WORKER)
        * Frame 5: command (MDPW_REPORT_CLOUD)
		* Frame 6: service name
		* Frame 7: data

* dzbroker: dzbroker recevie from **cloudbe**, message from peer's cloudfe
    1. message receive at cloudbe, from peer's cloudfe (above 5, scene 3)
        * Frame 1: client address
        * Frame 2: empty
        * Frame 3: header (MDPW_WORKER)
        * Frame 4: command (MDPW_REPORT_CLOUD)
		* Frame 5: service name
		* Frame 6: data
    2. after cloudbe recevied message from peer's cloudfe, envelop it and send to **localfe**
        * Frame 1: client address
        * Frame 2: empty
        * Frame 3: header (MDPC_CLIENT)
        * Frame 4: command (MDPC_REPORT)
		* Frame 5: service name
		* Frame 6: data

* constrant
    1. client header
        * MDPC_CLIENT         "MDPC0X"
    2. client command
        * MDPC_REQUEST        "\001"
        * MDPC_REPORT         "\002"
        * MDPC_NAK            "\003"
        * MDPC_REPOST         "\004"
    3. worker header
        * MDPW_WORKER         "MDPW0X"
    4. worker command
		* MDPW_READY          "\001"
		* MDPW_REQUEST        "\002"
		* MDPW_REPORT         "\003"
		* MDPW_HEARTBEAT      "\004"
		* MDPW_DISCONNECT     "\005"
		* MDPW_REPOST         "\006"
		* MDPW_REPORT_LOCAL   "\007"
		* MDPW_REPORT_CLOUD   "\010"

* reference
    * [ZeroMQ](http://zeromq.org/)
    * [7/MDP - Majordomo Protocol](http://rfc.zeromq.org/spec:7)

