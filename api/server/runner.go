package server

import (
	"bytes"
	"io"
	"net/http"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/fnproject/fn/api"
	"github.com/fnproject/fn/api/agent"
	"github.com/fnproject/fn/api/common"
	"github.com/fnproject/fn/api/models"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// handleFunctionCall executes the function, for router handlers
func (s *Server) handleFunctionCall(c *gin.Context) {
	err := s.handleFunctionCall2(c)
	if err != nil {
		handleErrorResponse(c, err)
	}
}

// handleFunctionCall2 executes the function and returns an error
// Requires the following in the context:
// * "app_name"
// * "path"
func (s *Server) handleFunctionCall2(c *gin.Context) error {
	ctx := c.Request.Context()
	var p string
	r := ctx.Value(api.Path)
	if r == nil {
		p = "/"
	} else {
		p = r.(string)
	}

	appID := c.MustGet(api.AppID).(string)
	app, err := s.agent.GetAppByID(ctx, appID)
	if err != nil {
		return err
	}

	// gin sets this to 404 on NoRoute, so we'll just ensure it's 200 by default.
	c.Status(200) // this doesn't write the header yet

	return s.serve(c, app, path.Clean(p))
}

var (
	bufPool = &sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}
)

type CallResponse struct {
	Status      int
	ContentType string
	Data        interface{}
}

// CallFunction exposed to become the API extension, to let API listeners to be capable to call a functions
// defined by app and path
func (s *Server) CallFunction(app *models.App, path string, req *http.Request,
	respWriter http.ResponseWriter) (*string, io.Reader, error) {

	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	writer := syncResponseWriter{
		Buffer:  buf,
		headers: respWriter.Header(), // copy ref
	}
	defer bufPool.Put(buf) // TODO need to ensure this is safe with Dispatch?

	// GetCall can mod headers, assign an id, look up the route/app (cached),
	// strip params, etc.
	// this should happen ASAP to turn app name to app ID

	// GetCall can mod headers, assign an id, look up the route/app (cached),
	// strip params, etc.

	call, err := s.agent.GetCall(
		agent.WithWriter(&writer), // XXX (reed): order matters [for now]
		agent.FromRequest(app, path, req),
	)
	if err != nil {
		return nil, nil, err
	}
	model := call.Model()
	{ // scope this, to disallow ctx use outside of this scope. add id for handleErrorResponse logger
		ctx, _ := common.LoggerWithFields(req.Context(), logrus.Fields{"id": model.ID})
		req = req.WithContext(ctx)
	}

	if model.Type == "async" {
		// TODO we should push this into GetCall somehow (CallOpt maybe) or maybe agent.Queue(Call) ?
		if req.ContentLength > 0 {
			buf.Grow(int(req.ContentLength))
		}
		_, err := buf.ReadFrom(req.Body)
		if err != nil {
			return nil, nil, models.ErrInvalidPayload
		}
		model.Payload = buf.String()

		// TODO idk where to put this, but agent is all runner really has...
		err = s.agent.Enqueue(req.Context(), model)
		if err != nil {
			return nil, nil, err
		}
		return &model.ID, nil, nil
	}

	err = s.agent.Submit(call)
	if err != nil {
		// NOTE if they cancel the request then it will stop the call (kind of cool),
		// we could filter that error out here too as right now it yells a little
		if err == models.ErrCallTimeoutServerBusy || err == models.ErrCallTimeout {
			// TODO maneuver
			// add this, since it means that start may not have been called [and it's relevant]
			respWriter.Header().Add("XXX-FXLB-WAIT", time.Now().Sub(time.Time(model.CreatedAt)).String())
		}
		return nil, nil, err
	}

	// if they don't set a content-type - detect it
	if writer.Header().Get("Content-Type") == "" {
		// see http.DetectContentType, the go server is supposed to do this for us but doesn't appear to?
		var contentType string
		jsonPrefix := [1]byte{'{'} // stack allocated
		if bytes.HasPrefix(buf.Bytes(), jsonPrefix[:]) {
			// try to detect json, since DetectContentType isn't a hipster.
			contentType = "application/json; charset=utf-8"
		} else {
			contentType = http.DetectContentType(buf.Bytes())
		}
		writer.Header().Set("Content-Type", contentType)
	}

	writer.Header().Set("Content-Length", strconv.Itoa(int(buf.Len())))

	if writer.status > 0 {
		respWriter.WriteHeader(writer.status)
	}

	return nil, writer, nil
}

// TODO it would be nice if we could make this have nothing to do with the gin.Context but meh
// TODO make async store an *http.Request? would be sexy until we have different api format...
func (s *Server) serve(c *gin.Context, app *models.App, path string) error {
	callID, syncWriter, err := s.CallFunction(app, path, c.Request, c.Writer)
	if err != nil {
		return err
	}

	if callID != nil {
		c.JSON(http.StatusAccepted, map[string]string{"call_id": *callID})
		return nil
	}

	if syncWriter != nil {
		io.Copy(c.Writer, syncWriter)
		return nil
	}

	return nil
}

var _ http.ResponseWriter = new(syncResponseWriter)

// implements http.ResponseWriter
// this little guy buffers responses from user containers and lets them still
// set headers and such without us risking writing partial output [as much, the
// server could still die while we're copying the buffer]. this lets us set
// content length and content type nicely, as a bonus. it is sad, yes.
type syncResponseWriter struct {
	headers http.Header
	status  int
	*bytes.Buffer
}

func (s *syncResponseWriter) Header() http.Header  { return s.headers }
func (s *syncResponseWriter) WriteHeader(code int) { s.status = code }
