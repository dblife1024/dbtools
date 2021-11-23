package core

type Core struct {
	Version string
}

var cxt Core

func New() Core {

	cxt.Version = "0.1"
	return cxt
}

func (c *Core) Run() {

	//suport plug
	plug := NewPlug()

	//parse command
	cmd := NewCmd(plug)
	cmd.Run()
}
