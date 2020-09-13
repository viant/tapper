package consumer

import "net/http"

//Server represents consumer server
type Server struct {
	*http.Server
	service *Service
}

//ServeHTTP servers HTTP
func (s *Server) ServeHTTP(writer http.ResponseWriter, httpRequest *http.Request) {
	httpRequest.ParseForm()
	request := &Request{
		URL:    httpRequest.RequestURI,
		Params: make(map[string]string),
	}
	if len(httpRequest.Form) > 0 {
		for key := range httpRequest.Form {
			request.Params[key] = httpRequest.Form.Get(key)
		}
	}
	err := s.service.Consume(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
}

//NewServer creates a new service
func NewServer(port string, service *Service) *Server {
	result := &Server{service: service}
	mux := http.NewServeMux()
	mux.Handle("/", result)
	result.Server = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}
	return result
}
