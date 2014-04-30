package raft

import (
	"encoding/json"
	"fmt"
	"testing"
zmq "github.com/pebbe/zmq4"
	"time"

	"os"
)



type client struct {
socket *zmq.Socket
addresses map[int]string
}


func (c *client) send(msg request) reply {

fmt.Println(msg)
message, _ := json.Marshal(msg)
fmt.Println("ssending--")
c.socket.SendBytes(message, 0)

resp, err := c.socket.RecvBytes(0)
fmt.Println("--------------------------------------------------------------------------------------------------------------------------000000000000")
var rep reply
json.Unmarshal(resp, &rep)
if err != nil {
fmt.Println(err)
}
return rep

}

///specially written to write test code,..can be written in main as well
func (c *client) prepare(op int, id int, r int, a string, p string) reply {
var m request

m = request{op, id, r, a, p}
var rep reply
for{
rep = c.send(m)

if (rep.Op==9){
fmt.Println("-----------------------------++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
fmt.Println(c.addresses[rep.Idirect])
err:=c.socket.Connect(c.addresses[rep.Idirect])
fmt.Println(err)
}else{
break
}
}
return rep
}



//import "log"


func initialise_kvstore1(s Server){
run(s,"tcp://127.0.0.1:8001")
}

func initialise_kvstore2(s Server){
run(s,"tcp://127.0.0.1:8002")
}

func initialise_kvstore3(s Server){
run(s,"tcp://127.0.0.1:8003")
}

func initialise_kvstore4(s Server){
run(s,"tcp://127.0.0.1:8004")
}

func initialise_kvstore5(s Server){
run(s,"tcp://127.0.0.1:8005")
}

func initialise_kvstore6(s Server){
run(s,"tcp://127.0.0.1:8006")
}

func initialise_kvstore7(s Server){
run(s,"tcp://127.0.0.1:8007")
}



func putgettest(t *testing.T,s3 Server){



////***************** testing randomly creates users,keep details(put),delete details,remove users phase(delete)***********************************************////


var cl client
cl=client{addresses:map[int]string{1:"tcp://127.0.0.1:8001",2:"tcp://127.0.0.1:8002",3:"tcp://127.0.0.1:8003",4:"tcp://127.0.0.1:8004",5:"tcp://127.0.0.1:8005",6:"tcp://127.0.0.1:8006",7:"tcp://127.0.0.1:8007"}}
cl.socket, _ = zmq.NewSocket(zmq.REQ)
var cl1 client
cl1=client{addresses:map[int]string{1:"tcp://127.0.0.1:8001",2:"tcp://127.0.0.1:8002",3:"tcp://127.0.0.1:8003",4:"tcp://127.0.0.1:8004",5:"tcp://127.0.0.1:8005",6:"tcp://127.0.0.1:8006",7:"tcp://127.0.0.1:8007"}}
err := cl.socket.Connect("tcp://127.0.0.1:8003")

fmt.Println(err)
fmt.Println("Initialisng the client........")

fmt.Println("User options 1.register 2.filldetails(roll,address,phone) 3.change_rollno 4.change_adress 5.change_ph 6.getdetails 7.deleteme")
cl1.socket, _ = zmq.NewSocket(zmq.REQ)

err1:=cl1.socket.Connect("tcp://127.0.0.1:8006")

fmt.Println(err1)
var op, id, r int
var a, p string


t1, _ := os.OpenFile("logitems1.txt", os.O_APPEND|os.O_WRONLY, 0600)


////****************        Randomly puts the users & deletes format(1.create 2.ADD details 6.GET Details 7,DELETE)   **************/////////
//  note:replies not handled  ----later part
var rep reply

//Create 1.operation 2.ID 3.Rollno 4.Address 5.Phone
cl.prepare(1, 116, 0, "", "")


cl1.prepare(1,119, 0, "", "")

cl.prepare(1, 120, 0, "", "")

cl.prepare(1, 121, 0, "", "")
cl1.prepare(2, 143, 89, "", "sddsd")

//Delete
cl.prepare(7, 119, 0, "", "")
cl1.prepare(1, 117, 0, "", "")
cl1.prepare(1, 118, 0, "", "")
cl.prepare(1, 122, 0, "", "")
cl.prepare(1, 123, 0, "", "")
cl.prepare(1, 124, 0, "", "")
cl.prepare(1, 125, 0, "", "")
cl.prepare(2, 116,11001, "ny", "9299239")
cl.prepare(2, 113, 0, "", "")
cl.prepare(2, 119,11002, "la", "8928392")
cl.prepare(2, 120, 11003, "ny", "9038121")
cl1.prepare(2, 234, 89, "", "sddsd")

op, id, r, a, p = 1, 112, r, a, p
cl.prepare(op, id, 0, "", "")
cl.prepare(1, 126, 0, "", "")

cl.prepare(2, 117,11009, "lon", "81271212")
cl.prepare(2, 118,11010, "par", "93256565")

op, id, r, a, p = 2, 121, 11004, "Del", "929321239"
cl.prepare(op, id, r, a, p)
op, id, r, a, p = 1, 12, 0, "", ""
cl.prepare(op, id, r, a, p)

cl1.prepare(2, 89, 89, "", "sddsd")

op, id, r, a, p = 6, 13, 0, "", ""
cl.prepare(op, id, r, a, p)


cl.prepare(7, 120, 0, "", "")
cl.prepare(6, 123, 0, "", "")



cl.prepare(1, 127, 0, "", "")
cl1.prepare(2, 127, 11005, "Mum", "89232397")

cl.prepare(6, 127, 0, "", "")

cl.prepare(2, 122, 11006, "Blore", "83291021")
cl.prepare(2, 123, 11007, "Hyd", "821912812")
cl1.prepare(2, 125, 11008, "Pun", "82938292")
cl1.prepare(2, 891, 8219, "", "sddsd")

select {

		case <-time.After(6* time.Second):
		
}



////********************************  testing query phase *****************************************************////



//////  The test can be verified at the logitems.txt file where the sequence is checked to be consistent 
//Expected ouput:119:0(deleted) 122:0(deleted) 123:Details 120:0(Deleted) 124:0(don't exist) 119:Details(create-put-get) 123:0(deleted recently) 127:Details  117:Details 117:0 (deleted)             

var s1 string
rep = cl1.prepare(6, 123,0, "", "")

s1=fmt.Sprintf("%d %d %s %s",rep.Id,rep.Roll,rep.Addr,rep.Ph)

	t1.WriteString(s1+"\n")

rep = cl1.prepare(6, 121, 0, "", "")
s1=fmt.Sprintf("%d %d %s %s",rep.Id,rep.Roll,rep.Addr,rep.Ph)

	t1.WriteString(s1+"\n")

rep = cl.prepare(6, 119, 0, "", "")

s1=fmt.Sprintf("%d %d %s %s",rep.Id,rep.Roll,rep.Addr,rep.Ph)

	t1.WriteString(s1+"\n")


//few Deletes
cl1.prepare(7, 119, 0, "", "")
cl1.prepare(7, 123, 0, "", "")


rep = cl.prepare(6, 122, 0, "", "")

s1=fmt.Sprintf("%d %d %s %s",rep.Id,rep.Roll,rep.Addr,rep.Ph)
	t1.WriteString(s1+"\n")
rep = cl.prepare(6, 124, 0, "", "")


s1=fmt.Sprintf("%d %d %s %s",rep.Id,rep.Roll,rep.Addr,rep.Ph)
	t1.WriteString(s1+"\n")
rep = cl.prepare(6, 116, 0, "", "")
s1=fmt.Sprintf("%d %d %s %s",rep.Id,rep.Roll,rep.Addr,rep.Ph)
	t1.WriteString(s1+"\n")
cl1.prepare(7, 123, 0, "", "")

rep = cl.prepare(6, 120, 0, "", "")

s1=fmt.Sprintf("%d %d %s %s",rep.Id,rep.Roll,rep.Addr,rep.Ph)
	t1.WriteString(s1+"\n")
rep = cl.prepare(6, 117, 0, "", "")

s1=fmt.Sprintf("%d %d %s %s",rep.Id,rep.Roll,rep.Addr,rep.Ph)
	t1.WriteString(s1+"\n")
rep = cl.prepare(6, 118, 0, "", "")

s1=fmt.Sprintf("%d %d %s %s",rep.Id,rep.Roll,rep.Addr,rep.Ph)
	t1.WriteString(s1+"\n")

rep = cl.prepare(6, 123, 0, "", "")

s1=fmt.Sprintf("%d %d %s %s",rep.Id,rep.Roll,rep.Addr,rep.Ph)
	t1.WriteString(s1+"\n")
rep = cl.prepare(6, 127, 0, "", "")
s1=fmt.Sprintf("%d %d %s %s",rep.Id,rep.Roll,rep.Addr,rep.Ph)
	t1.WriteString(s1+"\n")
cl1.prepare(1, 119, 0, "", "")
cl.prepare(2, 119,11002, "la", "8928392")
rep = cl.prepare(6, 119, 0, "", "")
s1=fmt.Sprintf("%d %d %s %s",rep.Id,rep.Roll,rep.Addr,rep.Ph)
	t1.WriteString(s1+"\n")
cl1.prepare(7, 117, 0, "", "")
rep = cl.prepare(6, 117, 0, "", "")
s1=fmt.Sprintf("%d %d %s %s",rep.Id,rep.Roll,rep.Addr,rep.Ph)
	t1.WriteString(s1+"\n")


}


func Test(t *testing.T) {


///////////////////////////////
/////initilase raft////////////
//////////////////////////////


	ch := make(chan bool)
	//ch1:=make(chan bool)
	s1:=New(1, "./config.json", ch)
	//initialise_kvstore(s1)
        s2:=New(2, "./config.json", ch)
	//initialise_kvstore(s2)	
	 s3:=New(3, "./config.json", ch)
	//initialise_kvstore(s3)
	s4:=New(4, "./config.json", ch)
	//initialise_kvstore(s4)
	s5:=New(5, "./config.json", ch)
	//initialise_kvstore(s5)
	s6:=New(6, "./config.json", ch)
	//initialise_kvstore(s6)
	s7:=New(7, "./config.json", ch)
	

///////////////////////////////
/////**initilase kvstores**////////////
//////////////////////////////


initialise_kvstore1(s1)
initialise_kvstore2(s2)
initialise_kvstore3(s3)
initialise_kvstore4(s4)
initialise_kvstore5(s5)
initialise_kvstore6(s6)
initialise_kvstore7(s7)



///////////////////////////////
/////**random wait**////////////
//////////////////////////////

select {

		case <-time.After(1 * time.Second):
			
}



//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////                        ***Main Tests*****            ///////////////////////////////////////////////////////////////////////////////////////
//Test Scenario///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//	1.Random restart  											     /////////////////////////////
//	2.Put & Get Tests                                                                                            /////////////////////////////
//           i.put some values on any of the server randomly                                                         /////////////////////////////
///		.the raft internally replicates on all the machines and commits to databases on the replicated servers//////////////////////////// 
///		.After a series of random puts and gets the A random server queries for the same data instance        ////////////////////////////
///                                                                                                                   ////////////////////////////
///	     o.A succesful replication shows the database is consistent and the data is queried succesfullu           ////////////////////////////
///														      ////////////////////////////
///	3.After a regular intervals the servers are started and stopped randomly                                      ////////////////////////////
///														      ////////////////////////////
///       4.The raft continues to operate succesfully in case of server shutdowns and restarts proving the effectiveness of the raft ///////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////






go putgettest(t,s3)





//manual test


		


	select {

		case <-time.After(3 * time.Second):
                     
			

//stopping the leader		}
        
        select {

		case <-time.After(2 * time.Second):
                        fmt.Println("leader")
			
			ch <-true

		}

        
//random wait----new leader gets elected   

	select {

		case <-time.After(1 * time.Second):
                     
			

		}
        
         
//stopping yet another server again	
		select {

		case <-time.After(2 * time.Second):

			ch <-true


		}
//restarted the stopped servers
s1.start()
s3.start()

select {

		case <-time.After(1 * time.Second):
                     
			

		}
        
//killing goes
	select {

		case <-time.After(1 * time.Second):
                     
			ch<-true

		}


//the loop starts until the system halts

for{
select {

		case <-time.After(4 * time.Second):
//s1.Out()<-"add123"
		ch <-true


		}
	
	
}
	



	

}

}
