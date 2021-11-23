package build

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/google/gopacket"
)

type Redis struct {
	port    int
	version string
	cmd     chan string
	done    chan bool
}

const (
	Port    int    = 6379
	Version string = "0.1"
	CmdPort string = "-p"
)

var redis = &Redis{
	port:    Port,
	version: Version,
}

func NewInstance() *Redis {
	return redis
}

func (red Redis) ResolveStream(net, transport gopacket.Flow, r io.Reader) {

	buf := bufio.NewReader(r)
	var cmd string
	for {

		line, _, _ := buf.ReadLine()

		if len(line) == 0 {
			buff := make([]byte, 1)
			_, err := r.Read(buff)
			if err == io.EOF {
				red.done <- true
				return
			}
		}
		cmd = ""
		cmd += time.Now().Format("2006-01-02 15:04:05")
		srcPort, dstPort := transport.Endpoints()
		srcIp, dstIp := net.Endpoints()
		cmd += fmt.Sprintf("|%v:%v => %v:%v|", srcIp, srcPort, dstIp, dstPort)

		if strings.EqualFold(transport.Src().String(), strconv.Itoa(red.port)) == true {
			errRep := red.resolveServer(line)
			if errRep {
				cmd += getNowStr(false)
				cmd += " " + string(line)
			} else {
				continue
			}
		} else {
			//Filtering useless data
			if !strings.HasPrefix(string(line), "*") {
				continue
			}
			cmd += getNowStr(true)
			cmd += red.resolveClient(line, buf)
		}

		fmt.Println(cmd)
	}
}

func (red *Redis) resolveClient(line []byte, buf *bufio.Reader) string {

	var cmd string
	var cmdCount = 0
	cmd = " "
	l := string(line[1])
	cmdCount, _ = strconv.Atoi(l)
	for j := 0; j < cmdCount*2; j++ {
		c, _, _ := buf.ReadLine()
		if j&1 == 0 {
			continue
		}
		cmd += " " + string(c)
	}

	return cmd
}

func (red *Redis) resolveServer(line []byte) bool {
	var errRep bool = false
	if strings.HasPrefix(string(line), "-ERR") {
		errRep = true
	}
	return errRep
}

/**
SetOption
*/
func (red *Redis) SetFlag(flg []string) {
	c := len(flg)
	if c == 0 {
		return
	}
	if c>>1 != 1 {
		panic("ERR : Redis num of params")
	}
	for i := 0; i < c; i = i + 2 {
		key := flg[i]
		val := flg[i+1]

		switch key {
		case CmdPort:
			port, err := strconv.Atoi(val)
			redis.port = port
			if err != nil {
				panic("ERR : Port error")
			}
			if port < 0 || port > 65535 {
				panic("ERR : Port(0-65535)")
			}
			break
		default:
			panic("ERR : redis's params")
		}
	}
}

/**
BPFFilter
*/
func (red *Redis) BPFFilter() string {
	return "tcp and port " + strconv.Itoa(redis.port)
}

/**
Version
*/
func (red *Redis) Version() string {
	return red.version
}
