package config

//MaxRetries max event firing retry limit
const MaxRetries = 100

//Event represents an rotation event
type Event struct {
	Command    string
	Args       []string
	URL        string
	Params     map[string]string
	MaxRetries int
}

//Init initialises an event
func (c *Event) Init() {
	if c.MaxRetries == 0 {
		c.MaxRetries = MaxRetries
	}
}
