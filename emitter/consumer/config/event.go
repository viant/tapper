package config

//Command represents log rotation consumer event command
type Command struct {
	URI  string
	Name string
	Args []string
}

//ExpandArgs expand args
func (c Command) ExpandArgs(params map[string]string) []string {
	var result = make([]string, 0)
	for i := range c.Args {
		value := c.Args[i]
		if value != "" && value[0] != '$' {
			result = append(result, value)
			continue
		}
		if val, ok := params[value[1:]]; ok {
			value = val
		}
		result = append(result, value)
	}
	return result
}
