package build

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/google/gopacket"
)

const (
	Port    = 5432
	Version = "0.1"
	CmdPort = "-p"
)

type Postgres struct {
	port    int
	version string
	source  map[string]*stream
}

type stream struct {
	packets chan *packet
}

type packet struct {
	isClientFlow bool
	seq          int
	length       int
	payload      []byte
}

var postgres *Postgres
var once sync.Once

func NewInstance() *Postgres {

	once.Do(func() {
		postgres = &Postgres{
			port:    Port,
			version: Version,
			source:  make(map[string]*stream),
		}
	})

	return postgres
}

func (m *Postgres) ResolveStream(net, transport gopacket.Flow, buf io.Reader) {

	//uuid
	uuid := fmt.Sprintf("%v:%v", net.FastHash(), transport.FastHash())
	//generate resolve's stream
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

func (m *Postgres) newPacket(net, transport gopacket.Flow, r io.Reader) *packet {
	//read packet
	var payload bytes.Buffer
	var seq uint8
	var err error

	if seq, err = m.resolvePacketTo(r, &payload); err != nil {
		return nil
	}
	//close stream
	if err == io.EOF {
		fmt.Println(net, transport, " close")
		return nil
	} else if err != nil {
		fmt.Println("ERR : Unknown stream", net, transport, ":", err)
	}

	//generate new packet
	var pk = packet{
		seq:     int(seq),
		length:  payload.Len(),
		payload: payload.Bytes(),
	}
	if transport.Src().String() == strconv.Itoa(m.port) {
		// server->client
		pk.isClientFlow = false
	} else {
		pk.isClientFlow = true
	}
	return &pk
}

func (m *Postgres) resolvePacketTo(r io.Reader, w io.Writer) (uint8, error) {
	return 0, nil
	/*
		header := make([]byte, 4)
		if n, err := io.ReadFull(r, header); err != nil {
			if n == 0 && err == io.EOF {
				return 0, io.EOF
			}
			return 0, errors.New("ERR : Unknown stream")
		}

		length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<12)

		var seq uint8
		seq = header[3]

		if n, err := io.CopyN(w, r, int64(length)); err != nil {
			return 0, errors.New("ERR : Unknown stream")
		} else if n != int64(length) {
			return 0, errors.New("ERR : Unknown stream")
		} else {
			return seq, nil
		}

		return seq, nil
	*/
}

func (stm *stream) resolve() {
	for {
		select {
		case packet := <-stm.packets:
			if packet.length != 0 {
				if packet.isClientFlow {
					stm.resolveClientPacket(packet.payload, packet.seq)
				} else {
					stm.resolveServerPacket(packet.payload, packet.seq)
				}
			}
		}
	}
}

func (stm *stream) resolveServerPacket(payload []byte, seq int) {
	if len(payload) == 0 {
		return
	}
	//fmt.Println(string(payload[1:]))
	return
}

func (stm *stream) resolveClientPacket(payload []byte, seq int) {
	if len(payload) == 0 {
		return
	}
	//fmt.Println(string(payload[1:]))
	return
}

func (m *Postgres) BPFFilter() string {
	return "tcp and port " + strconv.Itoa(m.port)
}

func (m *Postgres) Version() string {
	return Version
}

func (m *Postgres) SetFlag(flg []string) {

	c := len(flg)

	if c == 0 {
		return
	}
	if c>>1 == 0 {
		fmt.Println("ERR : Postgres Number of parameters")
		os.Exit(1)
	}
	for i := 0; i < c; i = i + 2 {
		key := flg[i]
		val := flg[i+1]

		switch key {
		case CmdPort:
			port, err := strconv.Atoi(val)
			m.port = port
			if err != nil {
				panic("ERR : port")
			}
			if port < 0 || port > 65535 {
				panic("ERR : port(0-65535)")
			}
			break
		default:
			panic("ERR : Postgres's params")
		}
	}
}
