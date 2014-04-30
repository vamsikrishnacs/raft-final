package raft


import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"io/ioutil"
	"os"
	"strconv"
	"time"
	 levigo "github.com/jmhodges/levigo"

        
)

const (
	BROADCAST = -1
)

type Envelope struct {
	// On the sender side, Pid identifies the receiving peer. If instead, Pid is
	// set to cluster.BROADCAST, the message is sent to all peers. On the receiver side, the
	// Id is always set to the original sender. If the Id is not found, the message is silently dropped
	Pid int

	// An id that globally and uniquely identifies the message, meant for duplicate detection at
	// higher levels. It is opaque to this package.
	MsgId int64

	// the actual message.
	Msg interface{}
}

/*type Server interface {
    // Id of this server
    Pid() int

    // array of other servers' ids in the same cluster
    Peers() []int


    // the channel to use to send messages to other peers
    // Note that there are no guarantees of message delivery, and messages
    // are silently dropped
    Outbox() chan *Envelope


    // the channel to receive messages from other peers.
    Inbox() chan *Envelope
}
*/

const (
	S = "stopped"
	F = "follower"
	C = "candidate"
	L = "leader"
)

type Server struct {
	serverID      int
	serverAddress string
	peers         []int
	peerAddress   map[string]string
	outbox        chan *Envelope
	inbox         chan *Envelope
	c1            int
	//persistent state
	votedFor    int
	currentTerm uint64
	state       string
	
	c chan *ev

		
	Electiontimeout   time.Duration
	Heartbeatinterval time.Duration
	log *Log
	
        peersock      []*zmq.Socket
    		// Mailbox for state machine layer above to send commands of any
   		// kind, and to have them replicated by raft.  If the server is not
   		// the leader, the message will be silently dropped.
   	raftOut chan interface{}

   		//Mailbox for state machine layer above to receive commands. These
   		//are guaranteed to have been replicated on a majority
   	raftIn  chan *LogItem
	Inc chan *LogItem
	AEresponses int
	leaderid int
        startch chan bool
	dbs DbConnection
        //currentIndex uint64


	
}


func (s *Server) manageLog() {
	select{
	case q:=<-s.raftOut:
	         fmt.Println(q)
		fmt.Println("got")
		 
	}
}



type ev struct {
	req interface{}
	t   string
}

type Appendentryreq struct {
	Term     uint64
	Leaderid int
	PrevLogIndex uint64 // index for the logentry immediately precedding new one.
	PrevLogTerm uint64 // term of the prevLogEntry
	Entries LogEntry
	LeaderCommit uint64

}

type Requestvotereq struct {
	Term        uint64
	Candidateid int
	LastLogIndex uint64
  	LastLogTerm uint64
}

type response struct {
	Updateterm uint64
	Success    bool
	Type int
}

func init() {
	gob.Register(request{})
	gob.Register(LogEntry{})
	gob.Register(Appendentryreq{})
	gob.Register(Requestvotereq{}) // give it a dummy VoteRequestObject.
	gob.Register(response{})
        gob.Register(ff{})
}

type cc struct {
	Ser               map[string]string
	Send              int
	Receive           int
	Peer              []int
	Electiontimeout   []time.Duration
	Heartbeatinterval time.Duration
}

//log---
type Log struct {
file *os.File
path string
entries []LogEntry
commitIndex uint64
//mutex sync.RWMutex
nextIndex map[int]uint64 // index of the next log entry to send to server for replication, initialized to leader lastlog index +1
matchIndex map[int]uint64 

lastLogIndex uint64
lastLogTerm  uint64
flag int
ch1 chan int
pid int
commit int



}

	// Creates a new log.

func newLog(id int) *Log {
i:=strconv.Itoa(id)
s,_:=os.Create("log"+i)
return &Log{
commitIndex:1,	
file: s,
entries: make([]LogEntry, 1000),

lastLogIndex:0,
lastLogTerm:0,
nextIndex:map[int]uint64{1:0,2:0,3:0,4:0,5:0,6:0,7:0},
matchIndex:map[int]uint64{1:0,2:0,3:0,4:0,5:0,6:0,7:0},
ch1:make(chan int, 100),
commit:0,
}

}





func (s *Server) initializeNextAndMatchIndex() {
peers := s.peers
for i := 0; i < len(peers); i++ {
s.log.nextIndex[peers[i]] = s.log.lastLogIndex + 1
}
fmt.Println("initindex----------------")
fmt.Println(s.serverID)
fmt.Println(s.log.nextIndex)

// initailizing match index
for i := 0; i < len(peers); i++ {
s.log.matchIndex[peers[i]] = 0
}
}


//log--entry
//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------


type LogItem struct {
Index uint64
Data interface{}
}



type LogEntry struct {
	    // An index into an abstract 2^64 size array
    //Index  int64
    
    Term uint64
	    // The data that was supplied to raft's inbox
    Data    interface{}
}


// Creates a new log entry associated with a log.


func newLogEntry(term uint64,data interface{}) (LogEntry, error) {

e := LogEntry{term,data}
return e, nil
}

func (e *Log)getEntry(index uint64) LogEntry {
return e.entries[index]
}

//OutRaft

func (s *Server) Outraft() {

done:=make(chan bool)
done1:=make(chan bool)

next:=1



for{
       select{
	      case  data:=<-s.raftOut:
var l uint64
l=0
		

	peers := s.peers
fmt.Println(peers)
        if s.state==L{
					go s.handlebox(done,done1)

		for i := 0; i < len(peers); i++ {
			if s.serverID!=peers[i]{
				if s.log.nextIndex[peers[i]]<=s.log.lastLogIndex{ 
              			  l+= s.log.lastLogIndex-s.log.nextIndex[peers[i]]+1 }
		}

		}
		

                

			if next==1{
				  next=0
				fmt.Println("nexrt")

				 s.writelog(data,done,done1)
			}else{
		      
				select 
			{        
					
				 
			case <-done:	

				
					fmt.Println(s.serverID)				
					fmt.Println(s.state)		
					if(s.state==L){
	       
			              
                                         str,_:=data.(string)
					fmt.Println("~~~~~~~~~~~~~~~`=======[================================================================================")
					fmt.Println(str)
				     s.writelog(data,done,done1)
					
							
			
		    		         }
			  	}
			}
		}else{


			fmt.Println("not leader")
			fmt.Println(s.leaderid)
			fmt.Println("leader")
			
			t:=s.leaderid
			s.raftIn<-&LogItem{0,t}
		}		
}
}

}


func encode(e uint64)[]byte{
buf_enc := new(bytes.Buffer)
enc := gob.NewEncoder(buf_enc)
enc.Encode(e)
tResult := buf_enc.Bytes()
result := make([]byte, len(tResult))
return result
}

func encodek(e1 LogEntry)[]byte{

mCache := new(bytes.Buffer)
	encCache := gob.NewEncoder(mCache)
	encCache.Encode(e1)
return mCache.Bytes()
}



//writetofile----log
func (s *Server) writelog(data interface{},done chan bool,done1 chan bool) {


var s22 string
		s.log.lastLogIndex+=1
		s.log.entries[s.log.lastLogIndex],_=newLogEntry(s.currentTerm,data)

switch req := data.(type) {

			case request:
				s22=fmt.Sprintf("%d %d %d %s %s",req.Op,req.Id,req.Roll,req.Addr,req.Ph)



}
s11:=fmt.Sprintf("%d %d",s.log.lastLogIndex,s.currentTerm)


		s.log.file.WriteString(s11+" "+s22+"\n")



str,ok:=data.(string)
if ok{ 


		
		s1:=fmt.Sprintf("%d %d",s.log.lastLogIndex,s.currentTerm)
		

		fmt.Println(s1+":"+str)
		s.log.file.WriteString(s1+" "+str+"\n")

//encode

result:=encode(s.log.lastLogIndex)
result1:=encodek(s.log.entries[s.log.lastLogIndex])
		s.dbs.Put(result,result1)
		//write to leveldb

		fmt.Println(s.dbs.Get(result))
r1,_:=s.dbs.Get(result)
	buf_dec := new(bytes.Buffer)
dec := gob.NewDecoder(buf_dec)
buf_dec.Write(r1)
var key LogEntry
dec.Decode(&key)
fmt.Println(key)
		
	fmt.Println(s.log.lastLogIndex)



    

}
           s.sendlog(done,done1)
}

func (s *Server) sendlog(done chan bool,done1 chan bool)  {
fmt.Println(s.log.nextIndex)
s.log.flag=1
last:=0
        
	peers := s.peers
        
        //s.initializeNextAndMatchIndex()
	for i := 0; i < len(peers); i++ {

		fmt.Println("&")
		fmt.Println(s.log.lastLogIndex)
                fmt.Println(s.log.nextIndex)
				
			if(s.log.lastLogIndex+1> s.log.nextIndex[peers[i]] && s.serverID!=peers[i]){	

			t:= s.log.nextIndex[peers[i]] 

				req:=Appendentryreq{s.currentTerm, s.serverID,(t-1),(s.log.entries[t-1].Term),s.log.entries[t],s.log.commitIndex}

                        	
				s.sendpAppendEntry(req,peers[i])
                       		s.log.flag=0
                        	
			
				if i==(len(peers)-1){
					last=1

				}

				select {

				case <-s.log.ch1:

					if(s.log.flag==1){
					

                      				 //  s.log.nextIndex[peers[i]]++ 
 					       	 s.log.nextIndex[s.log.pid]++ 
				  		}
					if(s.log.flag==2){
								

                      				 //  s.log.nextIndex[peers[i]]++ 
 					       		 s.log.nextIndex[s.log.pid]-- 
j:= s.log.nextIndex[peers[i]] 
hl:=0
						for ;;{


							if(hl==1){

								if(s.log.lastLogIndex<s.log.nextIndex[s.peers[i]]){	
									
									break
								}}

							

							 j= s.log.nextIndex[peers[i]] 


			         req1:=Appendentryreq{s.currentTerm, s.serverID,j-1,(s.log.entries[j-1].Term),s.log.entries[j],s.log.commitIndex} 
						        fmt.Println(req1) 

							s.sendpAppendEntry(req1,peers[i])


							


									select {

									case <-s.log.ch1:
	
               				                   		
										if(s.log.flag==1){
                      				 	
 											s.log.nextIndex[s.log.pid]++ 

											hl=1
               				                				   
							
											   
						  				}

										if(s.log.flag==2){
								                

                      				
 					       		 				s.log.nextIndex[s.log.pid]-- 
				  						}
									}
							}

							







				  		}


						if(last==1){

							fmt.Println("go________________________________________________________________________+")
							fmt.Println(s.log.nextIndex)
								done1<-true
								go pl(done)
							}
                                 }
}

				
			
		
		
			
         }

if s.log.commit==1{
s.log.commitIndex=s.log.lastLogIndex

s.raftIn<-&LogItem{s.log.lastLogIndex,s.log.entries[s.log.lastLogIndex].Data}
}


}


func pl(done chan bool){
	
		done<-true
}


func (s *Server) handlebox(done chan bool,done1 chan bool) {
var l uint64
l=0

s.AEresponses=0
if s.state==L{
                   for{
                       select{

                       case request := <-s.Inbox():
           
			switch res:=request.Msg.(type) {

			case response:
				

				if res.Type==1{
					l++
					
					if res.Success==true{
						s.AEresponses++
						if(s.AEresponses>=4){
 							s.log.commit=1
						}
						s.log.ch1<-1
                                                s.log.flag=1
						s.log.pid=request.Pid
					if(l==(6)){
                                                    l=0
						    


						
								l=0
							


						}

					}
					
					if res.Success==false{
						
						s.log.ch1<-1
                                                s.log.flag=2
						s.log.pid=request.Pid
						//return

					}
				}
                
                            }
                       case <-time.After(10 *time.Millisecond):
				
				s.log.ch1<-1
                                                s.log.flag=0
				l++

			case <-done1:
				return


			}
}
}
}








//Creating the cluster

func New(id int, path string, ch chan bool) Server {

	fmt.Println("INITIALISING SERVER")
	fmt.Println(id)
	//1.reading the configuration file
	file, e := ioutil.ReadFile(path)
	if e != nil {
		fmt.Printf("File error: %v\n", e)
		os.Exit(1)
	}

	var c cc
	json.Unmarshal(file, &c)
	fmt.Println(c)
	id1 := strconv.Itoa(id)
	//2.initialising

	o := make(chan *Envelope, 1000)
	i := make(chan *Envelope, 1000)
	 ro:= make(chan interface{},100)
         ri:= make(chan *LogItem, 100)
         rii:= make(chan *LogItem, 100)
	
        sc:=make(chan bool,10)
        var tempSocks = make([]*zmq.Socket, len(c.Peer))
        //2.1 create log
	

	s := Server{id, c.Ser[id1], c.Peer, c.Ser, o, i, 0, 0, 0, F, make(chan *ev), c.Electiontimeout[id-1], c.Heartbeatinterval,newLog(id),tempSocks,ro,ri,rii,0,0,sc,database1(id)}
        s.initializeNextAndMatchIndex()
        fmt.Println(s.log.nextIndex[4])
        s.connect()
	
	//3.handling server sockets
	go handleServerrep(&s)
	go handleServerreq(&s)
        
	go s.Outraft()	
	go s.loop(ch)
        
        

	return s
}

func (s *Server) stop(respChan chan bool, ch chan bool) {
	select {
	case <-ch:
		s.stopheartbeat(respChan)
		
		s.state = S
		t1, _ := os.OpenFile("term.txt", os.O_APPEND|os.O_WRONLY, 0600)
		t1.WriteString(" ##stopped## ")
		fmt.Println(" ##stopped## ")
		
	}

}


func (s *Server) loop(ch chan bool) {

	for s.state!=""  {
		st := s.state
		fmt.Println("started")
if(s.serverID==3){
fmt.Println("in here")
				fmt.Println(s.state)}
		//s.debugln("server.loop.run ", state)
		switch st {
		case F:
			s.followerLoop()
		case C:
			s.candidateLoop()
		case L:
			s.leaderLoop(ch)
		case S:
if(s.serverID==3){
fmt.Println("waiting")
				fmt.Println(s.state)}

			select{
			case c:=<-s.startch:
				fmt.Println(c)
				if c==false{

					fmt.Println("im starting")
					s.state=F
				}

			}
			//return
		}
	}

}

func (s *Server) startheartbeat(stop chan bool) {
	ticker := time.NewTicker(s.HeartbeatInterval())
	for {
		select {

		case <-stop:
			return

		case <-ticker.C:
			fmt.Println("tick")
			fmt.Println(s.serverID)
			req := Appendentryreq{Term:s.currentTerm, Leaderid:s.serverID}
			s.sendAppendEntry(req)

		}
	}

}

func (s *Server) stopheartbeat(stop chan bool) {
	stop <- true
}


func (s *Server) commitlog() {

for{
if s.log.commitIndex <= s.log.lastLogIndex{
s.Inc<-&LogItem{s.log.commitIndex,s.log.entries[s.log.commitIndex].Data}
s.log.commitIndex++
}else{

break
}

}
	
}


//controller
func (s *Server) leaderLoop(ch chan bool) {

	s.commitlog()

	//write term to disc  
        s.log.file.WriteString(s.state)
	t1, _ := os.OpenFile("term.txt", os.O_APPEND|os.O_WRONLY, 0600)
	t1.WriteString(" ##leader## "+"\n")
	sid := strconv.Itoa(s.serverID)
	tid := strconv.Itoa(int(s.currentTerm))
	t1.WriteString(" (" + sid + ",")
	t1.WriteString(tid + ")")
	//startheartbeat
	respChan := make(chan bool)
	go s.startheartbeat(respChan)
	s.initializeNextAndMatchIndex()
	
	for s.state == L {

		sid := strconv.Itoa(s.serverID)
		tid := strconv.Itoa(int(s.currentTerm))
		t1.WriteString(" (" + sid + ",")
		t1.WriteString(tid + ")")

		//s.state=S
		go s.stop(respChan, ch)

		//startheartbeat() 

		select {

             
		//election timeout
		 case <-time.After(5 * time.Second):
			s.currentTerm++
			

		}
	}
}

func (s *Server) followerLoop() {
	l1:=len(s.Inbox())

	for l:=0;l<l1;l++{
		<-s.Inbox()
	}

	for s.state == F {
              
if s.serverID==3{
              
}	
	select {
		//process RPC's
               
		case request := <-s.Inbox():

			switch req := request.Msg.(type) {

			case ff:
                             fmt.Println(req)
                        case Appendentryreq:
				
				resp := s.pAppendentry(req)

				s.sendAppendEntryresponse(request.Pid, resp)
			case Requestvotereq:
				resp := s.pRequestvote(req)
				s.sendRequestVoteresponse(request.Pid, resp)
			default:

			}
			//election timeout
		case <-time.After(s.ElectionTimeout()):
                        if(s.serverID==3){
				fmt.Println("timedout")}
			s.state = C
		}
	}
}

func (s *Server) candidateLoop() {

	dovote := true
	var votes int
	for s.state == C {
		if dovote {
			votes = 0
			// Increment current term, vote for self.
			s.currentTerm++

			s.votedFor = s.serverID
			votes = votes + 1

			req := Requestvotereq{Term:s.currentTerm,Candidateid:s.serverID,LastLogIndex:s.log.lastLogIndex,LastLogTerm:s.log.entries[s.log.lastLogIndex].Term}

			s.sendRequestVote(req)

			dovote = false
if(s.serverID==3){
				fmt.Println("candidate im 3dov")}
		}
		select {

		//process RPC's
		case request := <-s.Inbox():

			switch req := request.Msg.(type) {

			case response:
				t1, _ := os.OpenFile("term.txt", os.O_APPEND|os.O_WRONLY, 0600)

				sid := strconv.Itoa(s.serverID)
				tid := strconv.Itoa(votes)
				t1.WriteString(" *" + sid + ",")
				t1.WriteString(tid + "*")
				if req.Success == true {
 if(s.serverID==3){
				fmt.Println("voted")}
					votes++

					if votes >= 4 /*(len(s.peers)/2)*/ {

						s.state = L
					}
				} else {

					//s.state=F

				}

			case Appendentryreq:

				resp := s.pAppendentry(req)
				//processed entry results in state change C to F	
				s.sendAppendEntryresponse(request.Pid, resp)

			case Requestvotereq:
				resp := s.pRequestvote(req)
				s.sendRequestVoteresponse(request.Pid, resp)

			}

			//electiontimeout
		case <-time.After(s.ElectionTimeout()):

			dovote = false
		}

	}

}





//rpc replies

func (s *Server) sendAppendEntryresponse(pid int, resp response) {

//fmt.Println(s.serverID)

	s.outbox <- &Envelope{Pid: pid,MsgId:2, Msg: resp}
}





func (s *Server) sendRequestVoteresponse(pid int, resp response) {

	s.Outbox() <- &Envelope{Pid: pid,MsgId:2, Msg: resp}
}

//rpc's send

func (s *Server) sendRequestVote(req Requestvotereq) {

	s.Outbox() <- &Envelope{Pid: -1,MsgId:2, Msg: req}

}

func (s *Server) sendAppendEntry(req Appendentryreq) {

	s.outbox <- &Envelope{Pid: -1,MsgId:2, Msg: req}
}

func (s *Server) sendpAppendEntry(req Appendentryreq,pid int) {
     
	s.outbox <- &Envelope{Pid: pid,MsgId:2, Msg: req}
}


//dtect terms from req
//receivers implementation of requestvote  state=L/F 
func (s *Server) pRequestvote(req Requestvotereq) response {
	var s1 response

	//receiver
	if req.Term < s.currentTerm {
		s1 = response{s.currentTerm, false,0}

	}else if (s.log.entries[s.log.lastLogIndex].Term>req.LastLogTerm){
		
		s1 = response{s.currentTerm, false,0}
	}else {

		if (s.log.lastLogIndex>req.LastLogIndex){
		s1 = response{s.currentTerm, false,0}
		}else{
		s.currentTerm = req.Term
		s.state = F
		if s.votedFor == 0 || s.votedFor == req.Candidateid {
			s.votedFor = req.Candidateid
			s1 = response{s.currentTerm, true,0}
		}else {
			s1 = response{s.currentTerm, false,0}
		}
		}
	}
      
	return s1
}

//receivers implementation of Appendentry state=C/F

func (s *Server) pAppendentry(req Appendentryreq) response { //reqchan
        s.leaderid=req.Leaderid
	
	var s1 response
	//receiver
	if req.Term < s.currentTerm {
		s1 = response{s.currentTerm, false,0}
	} else {
		s.votedFor = 0
		s.currentTerm = req.Term
		s.state = F
		s1 = response{s.currentTerm, true,0}
	}
		//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		//Append any new entries not already in the log
		//If leaderCommit > commitIndex, set commitIndex =min(leaderCommit, last log index




        if req.LeaderCommit!=0{
       // fmt.Println(req	)

        if (s.log.entries[req.PrevLogIndex]==(LogEntry{}) || s.log.entries[req.PrevLogIndex].Term!=req.PrevLogTerm) && req.PrevLogIndex!=0 {	
	
                fmt.Println("emptylog")
                fmt.Println(s.log.lastLogIndex)
		s1=response{s.currentTerm,false,1}
	}else if req.Entries.Term!=s.log.entries[req.PrevLogIndex+1].Term &&s.log.entries[req.PrevLogIndex+1]!=(LogEntry{}){ //req.Entries.Term!=s.log.ent[req.LeaderCommit].Term{
		    s.log.entries[req.PrevLogIndex+1]=(LogEntry{})
                    fmt.Println(req.PrevLogIndex)
		    fmt.Println(s.log.lastLogIndex)
		    
		    for i:=req.PrevLogIndex+1;;i++{
			     
                                  if s.log.entries[i]!=(LogEntry{}){
        		          s.log.entries[i]=(LogEntry{})
				  }else{
					break
				  }
			     
		   	 }
		   
                    s1=response{s.currentTerm,false,1}
	}else{

		s.log.lastLogIndex+=1
			//a:=strconv.Itoa(s.log.commitIndex)
			//a1:=strconv.Itoa(s.currentTerm)
	        //s.log.lastLogIndex++
		
		s.log.entries[req.PrevLogIndex+1]=req.Entries
	        fmt.Println("In for pappend")
	       // s2:=fmt.Sprintf("%d %d",req.PrevLogIndex+1,req.Term)
	        s2:=fmt.Sprintf("%d %d",req.PrevLogIndex+1,req.Entries.Term)
result:=encode((req.PrevLogIndex+1))
result1:=encodek(s.log.entries[req.PrevLogIndex+1])
		s.dbs.Put(([]byte(result)),[]byte(result1))       
a:=req.Entries.Data
var s23 string
switch req := a.(type) {

			case request:
				s23=fmt.Sprintf("%d %d %d %s %s",req.Op,req.Id,req.Roll,req.Addr,req.Ph)



}
             		
                
		s.log.file.WriteString(s2+" "+s23+"\n")
	        fmt.Println(req)
		fmt.Println(req.Entries)
                s1=response{s.currentTerm,true,1}
	      } 
	     

	}
 
	return s1
}

//Cluster code--------
//---------Handles sending and receiving of RPCs

func (serv *Server) Pid() int {
	return serv.serverID
}

func (serv *Server) ElectionTimeout() time.Duration {
	return serv.Electiontimeout

}

func (serv *Server) HeartbeatInterval() time.Duration {
	return serv.Heartbeatinterval
}

// array of other servers' ids in the same cluster
func (serv *Server) Peers() []int {
	return serv.peers
}

// the channel to use to send messages to other peers
// Note that there are no guarantees of message delivery, and messages are silently dropped
func (serv *Server) Outbox() chan *Envelope {
	return serv.outbox
}

// the channel to receive messages from other peers.
func (s *Server) Inbox() chan *Envelope {
	return s.inbox
}


func (s * Server) Out() chan<- interface{} {
return s.raftOut
}

func (s * Server) In() <-chan *LogItem {
return s.raftIn
}
//SUBROUTINE RECEIVE

func handleServerrep(s *Server) {

	//1.point-point messaging

	socketrep, _ := zmq.NewSocket(zmq.REP)
	/*
	   err := socket.Connect(PROTOCOL + addr)

	*/
	socketrep.Bind("tcp://" + s.serverAddress)

	//socket.Bind("tcp://127.0.0.1:6000")
	s.c1 = 0
	for {
		//decoding
		msg, err := socketrep.RecvBytes(0)
		if err != nil {
			fmt.Println(err)
		}
		socketrep.Send("Ack ok", 0)
		var msg1 Envelope
		pCache := bytes.NewBuffer(msg)
		decCache := gob.NewDecoder(pCache)
		decCache.Decode(&msg1)

		s.inbox <- &msg1 //&Envelope{1,1,msg}
		s.c1 = s.c1 + 1

	}

}

//SUBROUTINE SEND

func handleServerreq(s *Server /*,pid int*/) {

	//send msgs 
	for {
		select {
		case envelope := <-s.Outbox():

			if envelope.Pid == -1 {

				Sendbroadcastmessage(envelope, s)
			} else {

				Sendmessage(envelope, s)
			}
		case <-time.After(2 * time.Second):
			fmt.Println("Waited and waited. Ab thak gaya\n")
		}
	}
}

func (s *Server) connect() {

	for j1 := range s.peers {
		j := strconv.Itoa(j1 + 1)

		if (j1 + 1) != s.serverID {

			sock, _ := zmq.NewSocket(zmq.REQ)
			s.peersock[j1] = sock

			//    fmt.Println("CONNECTED")
			err := s.peersock[j1].Connect("tcp://" + s.peerAddress[j])
			if err != nil {
				fmt.Println(err)
			}
		}

	}

}


func (s *Server) start() {
fmt.Println("starting")
s.startch<-false
fmt.Println("startred")


}

func Sendmessage(envlope *Envelope, s *Server) {

	peerid := (envlope.Pid)
	//1.point-point messaging

	envlope.Pid = s.serverID
	//ms, _ := json.Marshal(*envlope)
        encodeData := envlope
	mCache := new(bytes.Buffer)
	encCache := gob.NewEncoder(mCache)
	encCache.Encode(encodeData)

	
        _, err1 := s.peersock[peerid-1].SendBytes(mCache.Bytes(), 0)
	if err1 != nil {
		fmt.Println("error")
	}

	//fmt.Println("Sending")
	_, err := s.peersock[peerid-1].Recv(0)
	if err != nil {
		fmt.Println(err)
	}

}

func Sendbroadcastmessage(envlope *Envelope, s *Server) {

	//1.broad-cast messaging

	envlope.Pid = s.serverID
	//ms, _ := json.Marshal(*envlope)
        //Encode
	encodeData := envlope
	mCache := new(bytes.Buffer)
	encCache := gob.NewEncoder(mCache)
	encCache.Encode(encodeData)

	for j1 := range s.peers {
		if (j1 + 1) != s.serverID {
			//fmt.Println("Sending")
                         s.peersock[j1].SendBytes(mCache.Bytes(), 0)
			
			_, err := s.peersock[j1].Recv(0)
			if err != nil {
				fmt.Println(err)
			}
		}
	}

}



////////
////////                 database begin-----------------------------------------------------------------------------------------------------
////////




const (
LOG_DB = true
)

type DbConnection struct {
ro *levigo.ReadOptions
wo *levigo.WriteOptions
db *levigo.DB
itr *levigo.Iterator
}

// if database does not exist create a new data base or opens up the existing one

func initi() *DbConnection {
var conn DbConnection
return &conn
}



func (conn *DbConnection) open(path string) error {
opts := levigo.NewOptions()
opts.SetCache(levigo.NewLRUCache(3 << 10))
opts.SetCreateIfMissing(true)
var err error
conn.db, err = levigo.Open(path, opts)
conn.ro = levigo.NewReadOptions()
conn.wo = levigo.NewWriteOptions()
return err
}

func (conn *DbConnection) Get(key []byte) ([]byte, error) {
data, err := (conn.db).Get(conn.ro, key)
return data, err
}

func (conn *DbConnection) Put(key []byte, value []byte) error {
err := conn.db.Put(conn.wo, key, value)
return err
}

// resturn the highest value of the key
// if not value present return false
func (conn *DbConnection) GetLastValue() ([]byte, bool) {
conn.itr = conn.db.NewIterator(conn.ro)
conn.itr.SeekToLast()
if conn.itr.Valid() == false {
return nil, false
}
return conn.itr.Value(), true
}

// return lastKey
func (conn *DbConnection) GetLastKey() ([]byte, bool) {
conn.itr = conn.db.NewIterator(conn.ro)
defer conn.itr.Close()
conn.itr.SeekToLast()
if conn.itr.Valid() == false {
return nil, false
}
return conn.itr.Key(), true
}

func (conn *DbConnection) DeleteEntry(key []byte) error {
return conn.db.Delete(conn.wo, key)
}

func (conn *DbConnection) close() {
//log.Println("Closing connection")
conn.ro.Close()
conn.wo.Close()
conn.db.Close()
//log.Println("Sucessfully Closed connection")
}



////////
////////                 database end----------------------------------------------------------------------------------------------------------------------------
////////


type ff struct{

Id int 
Sa string
}

func database1(serverID int) DbConnection{
var db DbConnection
i:=strconv.Itoa(serverID)
err:= db.open("db"+i)
if err != nil {
fmt.Println("Could not open database %v\n", err)

}
//err = OpenConnection("../data/db")
err = db.Put([]byte("hi1"),[]byte("bye1"))
err = db.Put([]byte("hi2"),[]byte("bye2"))
lastKey , _ :=	db.Get([]byte("hi1"))
err = db.Put([]byte("hi"),[]byte("bye"))
fmt.Println(string(lastKey))
return db

}

