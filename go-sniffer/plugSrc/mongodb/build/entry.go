package build

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/google/gopacket"
)

const (
	Port    = 27017
	Version = "0.1"
	CmdPort = "-p"
)

type Mongodb struct {
	port    int
	version string
	source  map[string]*stream
}

type stream struct {
	packets chan *packet
}

type packet struct {
	isClientFlow bool //client->server
	tmpNet       gopacket.Flow
	tmpTransport gopacket.Flow

	messageLength int
	requestID     int
	responseTo    int
	opCode        int //request type

	payload io.Reader
}

var mongodbInstance *Mongodb

func NewInstance() *Mongodb {
	if mongodbInstance == nil {
		mongodbInstance = &Mongodb{
			port:    Port,
			version: Version,
			source:  make(map[string]*stream),
		}
	}
	return mongodbInstance
}

func (m *Mongodb) SetFlag(flg []string) {
	c := len(flg)
	if c == 0 {
		return
	}
	if c>>1 != 1 {
		panic("ERR : Mongodb Number of parameters")
	}
	for i := 0; i < c; i = i + 2 {
		key := flg[i]
		val := flg[i+1]

		switch key {
		case CmdPort:
			p, err := strconv.Atoi(val)
			if err != nil {
				panic("ERR : port")
			}
			mongodbInstance.port = p
			if p < 0 || p > 65535 {
				panic("ERR : port(0-65535)")
			}
			break
		default:
			panic("ERR : mysql's params")
		}
	}
}

func (m *Mongodb) BPFFilter() string {
	return "tcp and port " + strconv.Itoa(m.port)
}

func (m *Mongodb) Version() string {
	return m.version
}

func (m *Mongodb) ResolveStream(net, transport gopacket.Flow, buf io.Reader) {

	//uuid
	uuid := fmt.Sprintf("%v:%v", net.FastHash(), transport.FastHash())

	//resolve packet
	if _, ok := m.source[uuid]; !ok {

		var newStream = stream{
			packets: make(chan *packet, 100),
		}

		m.source[uuid] = &newStream
		go newStream.resolve()
	}

	//read bi-directional packet
	//server -> client || client -> server
	for {

		newPacket := m.newPacket(net, transport, buf)
		if newPacket == nil {
			return
		}

		m.source[uuid].packets <- newPacket
	}
}

func (m *Mongodb) newPacket(net, transport gopacket.Flow, r io.Reader) *packet {

	//read packet
	var packet *packet
	var err error
	packet, err = readStream(net, transport, r)

	//stream close
	if err == io.EOF {
		var msg = ""
		msg += time.Now().Format("2006-01-02 15:04:05")
		srcPort, dstPort := transport.Endpoints()
		srcIp, dstIp := net.Endpoints()
		msg += fmt.Sprintf("|%v:%v => %v:%v|", srcIp, srcPort, dstIp, dstPort)
		if transport.Src().String() == strconv.Itoa(m.port) {
			msg += GetNowStr(false)
		} else {
			msg += GetNowStr(true)
		}
		msg += " close"
		fmt.Println(msg)

		//fmt.Println(net, transport, " close")
		return nil
	} else if err != nil {
		var msg = ""
		msg += time.Now().Format("2006-01-02 15:04:05")
		srcPort, dstPort := transport.Endpoints()
		srcIp, dstIp := net.Endpoints()
		msg += fmt.Sprintf("|%v:%v => %v:%v|", srcIp, srcPort, dstIp, dstPort)
		if transport.Src().String() == strconv.Itoa(m.port) {
			msg += GetNowStr(false)
		} else {
			msg += GetNowStr(true)
		}
		msg += fmt.Sprintf("ERR : Unknown stream %s ", err)
		fmt.Println(msg)
		//fmt.Println("ERR : Unknown stream", net, transport, ":", err)
		return nil
	}

	//set flow direction
	if transport.Src().String() == strconv.Itoa(m.port) {
		// server->client
		packet.isClientFlow = false
	} else {
		packet.isClientFlow = true
	}

	return packet
}

func (stm *stream) resolve() {
	for {
		select {
		case packet := <-stm.packets:
			if packet.isClientFlow {
				stm.resolveClientPacket(packet)
			} else {
				stm.resolveServerPacket(packet)
			}
		}
	}
}

func (stm *stream) resolveServerPacket(pk *packet) {
	return
}

func (stm *stream) resolveClientPacket(pk *packet) {

	var msg = ""
	msg += time.Now().Format("2006-01-02 15:04:05")
	srcPort, dstPort := pk.tmpTransport.Endpoints()
	srcIp, dstIp := pk.tmpNet.Endpoints()
	msg += fmt.Sprintf("|%v:%v => %v:%v|", srcIp, srcPort, dstIp, dstPort)
	msg += GetNowStr(true)

	switch pk.opCode {

	case OP_UPDATE:
		zero := ReadInt32(pk.payload)
		fullCollectionName := ReadString(pk.payload)
		flags := ReadInt32(pk.payload)
		selector := ReadBson2Json(pk.payload)
		update := ReadBson2Json(pk.payload)
		_ = zero
		_ = flags

		msg += fmt.Sprintf(" [Update] [coll:%s] %v %v",
			fullCollectionName,
			selector,
			update,
		)

	case OP_INSERT:
		flags := ReadInt32(pk.payload)
		fullCollectionName := ReadString(pk.payload)
		command := ReadBson2Json(pk.payload)
		_ = flags

		msg += fmt.Sprintf(" [Insert] [coll:%s] %v",
			fullCollectionName,
			command,
		)

	case OP_QUERY:
		flags := ReadInt32(pk.payload)
		fullCollectionName := ReadString(pk.payload)
		numberToSkip := ReadInt32(pk.payload)
		numberToReturn := ReadInt32(pk.payload)
		_ = flags
		_ = numberToSkip
		_ = numberToReturn

		command := ReadBson2Json(pk.payload)
		selector := ReadBson2Json(pk.payload)

		msg += fmt.Sprintf(" [Query] [coll:%s] %v %v",
			fullCollectionName,
			command,
			selector,
		)

	case OP_COMMAND:
		database := ReadString(pk.payload)
		commandName := ReadString(pk.payload)
		metaData := ReadBson2Json(pk.payload)
		commandArgs := ReadBson2Json(pk.payload)
		inputDocs := ReadBson2Json(pk.payload)

		msg += fmt.Sprintf(" [Command] [DB:%s] [Cmd:%s] %v %v %v",
			database,
			commandName,
			metaData,
			commandArgs,
			inputDocs,
		)

	case OP_GET_MORE:
		zero := ReadInt32(pk.payload)
		fullCollectionName := ReadString(pk.payload)
		numberToReturn := ReadInt32(pk.payload)
		cursorId := ReadInt64(pk.payload)
		_ = zero

		msg += fmt.Sprintf(" [Query more] [coll:%s] [num of reply:%v] [cursor:%v]",
			fullCollectionName,
			numberToReturn,
			cursorId,
		)

	case OP_DELETE:
		zero := ReadInt32(pk.payload)
		fullCollectionName := ReadString(pk.payload)
		flags := ReadInt32(pk.payload)
		selector := ReadBson2Json(pk.payload)
		_ = zero
		_ = flags

		msg += fmt.Sprintf(" [Delete] [coll:%s] %v",
			fullCollectionName,
			selector,
		)

	case OP_MSG:

		zero := ReadInt32(pk.payload)
		kind := ReadByte(pk.payload)
		_ = zero

		switch kind {
		case msgKindBody:
			document := ReadBson2Json(pk.payload)
			msg += fmt.Sprintf("OP_MSG : %s",
				document,
			)
		case msgKindDocumentSequence:
			fmt.Println(pk.payload)
		/*
			start := d.i
			size, err := d.readInt32()
			if err != nil {
				logp.Err("An error occurred while parsing OP_MSG message: %s", err)
				return false, false
			}
			cstring, err := d.readCStr()
			if err != nil {
				logp.Err("An error occurred while parsing OP_MSG message: %s", err)
				return false, false
			}
			m.event["message"] = cstring
			var documents []interface{}
			for d.i < start+size {
				document, err := d.readDocument()
				if err != nil {
					logp.Err("An error occurred while parsing OP_MSG message: %s", err)
					return false, false
				}
				documents = append(documents, document)
			}
			m.documents = documents
			m.event["selector"],err =doc2str(documents)
		*/
		default:
			fmt.Printf("Unknown message kind: %v", kind)
			return
		}
	default:
		return
	}
	fmt.Println(msg)
	//fmt.Println(GetNowStr(true) + msg)
}

func readStream(net, transport gopacket.Flow, r io.Reader) (*packet, error) {

	var buf bytes.Buffer
	p := &packet{}

	p.tmpNet = net
	p.tmpTransport = transport

	//header
	header := make([]byte, 16)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	// message length
	payloadLen := binary.LittleEndian.Uint32(header[0:4]) - 16
	p.messageLength = int(payloadLen)

	// opCode
	p.opCode = int(binary.LittleEndian.Uint32(header[12:]))

	if p.messageLength != 0 {
		io.CopyN(&buf, r, int64(payloadLen))
	}

	p.payload = bytes.NewReader(buf.Bytes())

	return p, nil
}
