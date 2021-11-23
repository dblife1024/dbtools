package core

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

const InternalCmdPrefix = "--"
const (
	InternalCmdHelp = "help"
	InternalCmdEnv  = "env"
	InternalCmdList = "list"
	InternalCmdVer  = "ver"
	InternalDevice  = "dev"
)

var availablePlugName = [5]string{"mysql", "redis", "postgres", "http", "mongodb"}

type Cmd struct {
	Device     string
	plugHandle *Plug
}

func NewCmd(p *Plug) *Cmd {

	return &Cmd{
		plugHandle: p,
	}
}

//start
func (cm *Cmd) Run() {

	//print help
	if len(os.Args) <= 1 {
		cm.printHelpMessage()
		os.Exit(1)
	}

	//parse command
	firstArg := string(os.Args[1])
	if strings.HasPrefix(firstArg, InternalCmdPrefix) {
		cm.parseInternalCmd()
	} else {
		cm.parsePlugCmd()
	}
}

//parse internal command
//like --help, --env, --device
func (cm *Cmd) parseInternalCmd() {

	arg := string(os.Args[1])
	cmd := strings.Trim(arg, InternalCmdPrefix)

	switch cmd {
	case InternalCmdHelp:
		cm.printHelpMessage()
		break
	case InternalCmdEnv:
		//fmt.Println("External plug-in path : " + cm.plugHandle.dir)
		cm.plugHandle.PrintList()
		break
	case InternalCmdList:
		cm.plugHandle.PrintList()
		break
	case InternalCmdVer:
		fmt.Println(cxt.Version)
		break
	case InternalDevice:
		cm.printDevice()
		break
	default:
		cm.printHelpMessage()
		break

	}
	os.Exit(1)
}

//usage
func (cm *Cmd) printHelpMessage() {

	fmt.Println("==================================================================================")
	fmt.Println("[Usage]")
	fmt.Println("")
	fmt.Println("    go-sniffer [device] [plug] [plug's params(optional)]")
	fmt.Println()
	fmt.Println("    [exp]")
	fmt.Println("          go-sniffer eth0 redis    -p 6379      Capture redis packet")
	fmt.Println("          go-sniffer eth0 mysql    -p 3306      Capture mysql packet")
	fmt.Println("          go-sniffer eth0 mongodb  -p 27017     Capture mongodb packet")
	fmt.Println("          go-sniffer eth0 http     -p 80        Capture redis packet")
	fmt.Println("          go-sniffer eth0 postgres -p 5432      Capture postgres packet[Not completed yet]")
	fmt.Println("==================================================================================")
	fmt.Println("    go-sniffer --[command]")
	fmt.Println("               --help \"this page\"")
	fmt.Println("               --env  \"environment variable\"")
	fmt.Println("               --list \"Plug-in list\"")
	fmt.Println("               --ver  \"version\"")
	fmt.Println("               --dev  \"device\"")
	fmt.Println("    [exp]")
	fmt.Println("          go-sniffer --list \"show all plug-in\"")
	fmt.Println()
	fmt.Println("==================================================================================")
	cm.printDevice()
	fmt.Println("==================================================================================")
	fmt.Println("if you want sniffer postgres，you can go to https://github.com/dalibo/pgshark")
}

//print plug-in list
func (cm *Cmd) printPlugList() {
	l := len(cm.plugHandle.InternalPlugList)
	//l += len(cm.plugHandle.ExternalPlugList)
	fmt.Println("#    Number of plug-ins : " + strconv.Itoa(l))
}

//print device
func (cm *Cmd) printDevice() {
	ifaces, err := net.Interfaces()
	if err != nil {
		panic(err)
	}
	for _, iface := range ifaces {
		addrs, _ := iface.Addrs()
		for _, a := range addrs {
			if ipnet, ok := a.(*net.IPNet); ok {
				if ip4 := ipnet.IP.To4(); ip4 != nil {
					fmt.Println("[device] : " + iface.Name + " : " + iface.HardwareAddr.String() + "  " + ip4.String())
				}
			}
		}
	}
}

//Parameters needed for plug-ins
// really action
func (cm *Cmd) parsePlugCmd() {

	if len(os.Args) < 3 {
		fmt.Println("not found [Plug-in name]")
		fmt.Println("go-sniffer [device] [plug] [plug's params(optional)]")
		cm.printHelpMessage()
		os.Exit(1)
	}

	cm.Device = os.Args[1]
	plugName := os.Args[2]
	plugParams := os.Args[3:]
	isElement := func() bool {
		var isElement bool = false
		for _, element := range availablePlugName {
			if plugName == element {
				isElement = true
			}
		}
		return isElement
	}()
	if !isElement {
		fmt.Printf("not found [Plug-in name] %s \n", plugName)
		fmt.Println("go-sniffer [device] [plug] [plug's params(optional)]")
		cm.printHelpMessage()
		os.Exit(1)
	}

	cm.plugHandle.SetOption(plugName, plugParams)
}
