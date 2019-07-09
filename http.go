package kafka_connect

import (
	"context"
	"encoding/json"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"time"
)

type errorResponse struct {
	Code    int    `json:"error_code"`
	Message string `json:"message"`
}

type Http struct {
	server       *http.Server
	host         string
	connectorReg *Registry
}

func (h *Http) connectors(writer http.ResponseWriter, request *http.Request) {
	connectors, err := h.connectorReg.Connectors()
	if err != nil {
		h.error(writer, err)
		return
	}
	writer.Header().Set(`Content-Type`, `application/json`)
	h.write(writer, connectors)
}

func (h *Http) getConnector(writer http.ResponseWriter, request *http.Request) {

	connector, err := h.connectorReg.Connector(mux.Vars(request)[`name`])
	if err != nil {
		h.error(writer, err)
		return
	}
	writer.Header().Set(`Content-Type`, `application/json`)
	h.write(writer, connector.Config())
}

func (h *Http) createConnector(writer http.ResponseWriter, request *http.Request) {
	config := RunnerConfig{}
	if err := json.NewDecoder(request.Body).Decode(&config.Connector); err != nil {
		h.error(writer, err)
		return
	}

	connector, err := h.connectorReg.NewConnector(&config)
	if err != nil {
		h.error(writer, err)
		return
	}

	writer.Header().Set(`Content-Type`, `application/json`)
	h.write(writer, connector.Config())
}

func (h *Http) reConfigureConnector(writer http.ResponseWriter, request *http.Request) {

	config := RunnerConfig{}
	if err := json.NewDecoder(request.Body).Decode(&config.Connector); err != nil {
		h.error(writer, err)
		return
	}

	err := h.connectorReg.Reconfigure(mux.Vars(request)[`name`], &config)
	if err != nil {
		h.error(writer, err)
		return
	}

	writer.Header().Set(`Content-Type`, `application/json`)
	h.write(writer, config)
}

func (h *Http) Start() {
	r := mux.NewRouter()

	// Metrics handler
	r.Handle(`/metrics`, promhttp.Handler())

	r.HandleFunc(`/connectors`, h.connectors).Methods(http.MethodGet)

	r.HandleFunc(`/connectors`, h.createConnector).Methods(http.MethodPost)

	r.HandleFunc(`/connectors/{name}`, h.getConnector).Methods(http.MethodGet)

	r.HandleFunc(`/connectors/{name}/config`, func(writer http.ResponseWriter, request *http.Request) {

	}).Methods(http.MethodGet)

	r.HandleFunc(`/connectors/{name}/config`, h.reConfigureConnector).Methods(http.MethodPut)

	r.HandleFunc(`/connectors/{name}/status`, func(writer http.ResponseWriter, request *http.Request) {

	}).Methods(http.MethodGet)

	r.HandleFunc(`/connectors/{name}/restart`, func(writer http.ResponseWriter, request *http.Request) {

	}).Methods(http.MethodPost)

	r.HandleFunc(`/connectors/{name}/pause`, func(writer http.ResponseWriter, request *http.Request) {

	}).Methods(http.MethodPut)

	r.HandleFunc(`/connectors/{name}/resume`, func(writer http.ResponseWriter, request *http.Request) {

	}).Methods(http.MethodPut)

	r.HandleFunc(`/connectors/{name}/delete`, func(writer http.ResponseWriter, request *http.Request) {

	}).Methods(http.MethodPut)

	r.HandleFunc(`/connector-plugins`, func(writer http.ResponseWriter, request *http.Request) {

	}).Methods(http.MethodPut)

	h.server.Addr = h.host
	h.server.Handler = handlers.RecoveryHandler(handlers.PrintRecoveryStack(true))(r)
	err := h.server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		Logger.Fatal(`web server stopped due to : `, err)
	}
}

func (h *Http) write(w http.ResponseWriter, i interface{}) {
	byt, err := json.Marshal(i)
	if err != nil {
		h.error(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, err = w.Write(byt)
	if err != nil {
		Logger.Error(`kafka_connect.Http`, err)
	}
}

func (h *Http) error(w http.ResponseWriter, err error) {
	resp := errorResponse{
		Code:    http.StatusUnprocessableEntity,
		Message: err.Error(),
	}

	byt, err := json.Marshal(resp)
	if err != nil {
		Logger.Error(`kafka_connect.Http`, err)
		return
	}

	w.Header().Set(`Content-Type`, `application/json`)
	w.WriteHeader(http.StatusUnprocessableEntity)

	_, err = w.Write(byt)
	if err != nil {
		Logger.Error(`kafka_connect.Http`, err)
	}
}

func (h *Http) Stop() error {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	return h.server.Shutdown(ctx)
}
