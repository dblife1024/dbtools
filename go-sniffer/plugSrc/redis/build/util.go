package build

func getNowStr(isClient bool) string {
	var msg string
	if isClient {
		msg += "cli -> ser|"
	} else {
		msg += "ser -> cli|"
	}
	return msg
}
