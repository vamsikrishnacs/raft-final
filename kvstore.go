package raft
import (
"encoding/json"
"fmt"
zmq "github.com/pebbe/zmq4"
"sync"
"os"
)

type database struct {
mutex sync.Mutex
Map map[int]*mydetails
}





type mydetails struct {
Roll int
Address string
phone string
}

func (d *database) put(id int) bool {
fmt.Println("put")
_, ok := d.Map[id]
fmt.Println(ok)
return ok
}

func (d *database) createuser(id int) bool {

ok := d.put(id)
if !ok {
d.mutex.Lock()
d.Map[id] = &mydetails{0, "", ""}
d.mutex.Unlock()
}
return ok
}
func (d *database) putroll(id int, roll int) {
ok := d.put(id)
if ok {
d.mutex.Lock()
d.Map[id].Roll = roll
d.mutex.Unlock()
}
}

func (d *database) putAddress(id int, addr string) {
ok := d.put(id)
if ok {
d.mutex.Lock()
d.Map[id].Address = addr
d.mutex.Unlock()
}
}

func (d *database) putphone(id int, ph string) {
ok := d.put(id)
if ok {
d.mutex.Lock()
d.Map[id].phone = ph
d.mutex.Unlock()
}
}


func (d *database) getMydetails(id int) (*mydetails, bool) {
fmt.Println(id)
_, ok := d.Map[id]
var s *mydetails
if ok {
d.mutex.Lock()
s, _ = d.Map[id]
d.mutex.Unlock()
} else {
s = &mydetails{0, "", ""}
}
return s, ok
}

func (d *database) deleteMydetails(id int) {
d.mutex.Lock()
delete(d.Map, id)
d.mutex.Unlock()
}

type request struct {
Op int
Id int
Roll int
Addr string
Ph string
}

type reply struct {
Success bool
Id int
Roll int
Addr string
Ph string
Idirect int
Op int
}

func (d *database) statemachinecommit(s Server){
for{

select{

	case req:=<-s.Inc:
		fmt.Println("Data")
		fmt.Println(req.Data)

	switch q:=req.Data.(type) {

			case request:
				//s23=fmt.Sprintf("%d %d %d %s %s",req.Op,req.Id,req.Roll,req.Addr,req.Ph)
				fmt.Println("type")

				switch q.Op {

				case 1:
					d.createuser(q.Id)
				case 2:
					d.putroll(q.Id, q.Roll)
					d.putAddress(q.Id, q.Addr)
					d.putphone(q.Id, q.Ph)	
				case 6:
					d.getMydetails(q.Id)

				case 7:
					d.deleteMydetails(q.Id)
				}


			}


   }

}

}




type aa struct{
a int 
b int
}

func (d *database) parse(msg request,s Server) reply {
var r reply
switch msg.Op {

case 1:
//createuser
fmt.Println("In Create")
s.Out()<-msg
//s.Out()<-aa{1,2}
select{
	case d1:=<-s.In():
fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!%!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!############################################")	
					fmt.Println("out create")
	fmt.Println(d1)

 	str,_:=d1.Data.(string)
	

	if (d1.Index==0){
					fmt.Println("(((((((((((")

	fmt.Println(d1.Data)
					fmt.Println(")))))))))))")
	//r=reply{}	
		 	t,_:=d1.Data.(int)
	r=reply{false,0,0,"","",t,9}
	}else{
	fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!%!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!############################################")
	t1, _ := os.OpenFile("logitems.txt", os.O_APPEND|os.O_WRONLY, 0600)
	t1.WriteString(str+"\n")
	ok := d.createuser(msg.Id)
	r = reply{ok, msg.Id, 0, "", "",0,0}
	}
}

case 2:
//putdetails
fmt.Println("In put")
s.Out()<-msg

select{
	case d1:=<-s.In():
		if (d1.Index==0){
	 	t,_:=d1.Data.(int)
	r=reply{false,0,0,"","",t,9}
	}else{
 	str,_:=d1.Data.(string)
	t1, _ := os.OpenFile("logitems.txt", os.O_APPEND|os.O_WRONLY, 0600)
	t1.WriteString(str+"\n")
	

	d.putroll(msg.Id, msg.Roll)
	d.putAddress(msg.Id, msg.Addr)
	d.putphone(msg.Id, msg.Ph)
	r = reply{true, msg.Id, 0, "", "",0,0}
	}
}


case 6:
//getdetails
fmt.Println("In get")
s.Out()<-msg
select{
	case d1:=<-s.In():
	
		if (d1.Index==0){
	 	t,_:=d1.Data.(int)
		r=reply{false,0,0,"","",t,9}
		}else{

	 	str,_:=d1.Data.(string)
		t1, _ := os.OpenFile("logitems.txt", os.O_APPEND|os.O_WRONLY, 0600)
		t1.WriteString(str+"\n")
		s, ok := d.getMydetails(msg.Id)
		r = reply{ok, msg.Id, s.Roll, s.Address, s.phone,0,0}
		}
}

case 7:
//deletekey
fmt.Println("In delete")
s.Out()<-msg

select{
	case d1:=<-s.In():
		if (d1.Index==0){
	 	t,_:=d1.Data.(int)
			r=reply{false,0,0,"","",t,9}
		}else{
 		str,_:=d1.Data.(string)
		t1, _ := os.OpenFile("logitems.txt", os.O_APPEND|os.O_WRONLY, 0600)
		t1.WriteString(str+"\n")
	d.deleteMydetails(msg.Id)
	r = reply{true, msg.Id, 0, "", "",0,0}
	}
}


}
return r

}

func run(s Server,address string) {

socketrep, _ := zmq.NewSocket(zmq.REP)
fmt.Println("Im in run------------------------------------------------------------------------------------------------------------------------------------")
fmt.Println(s.serverAddress)
//socketrep.Bind("tcp://127.0.0.1:8003")
err:=socketrep.Bind(address)
fmt.Println("reciever....printing")
//fmt.Println(address)
//err:=socketrep.Bind("tcp://"+s.serverAddress)
//fmt.Println(s.serverAddress)
fmt.Println(err)
fmt.Println(address)
go receieve(socketrep,s)



}


func receieve(socketrep *zmq.Socket,s Server){
var d database
d.Map = make(map[int]*mydetails)
go d.statemachinecommit(s)
for {
fmt.Println("tcp://127.0.0.1:7001")
msg, err := socketrep.RecvBytes(0)

fmt.Println("recived")
if err != nil {
fmt.Println(err)
}
var req request
json.Unmarshal(msg, &req)
fmt.Println("parsing----------------------------------------------------------------------------------------")
fmt.Println(s.leaderid)
fmt.Println("parsing")
fmt.Println(req)
resp := d.parse(req,s)
fmt.Println(resp)
message, _ := json.Marshal(resp)
socketrep.SendBytes(message, 0)

}


}






