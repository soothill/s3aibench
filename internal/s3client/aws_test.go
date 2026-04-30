package s3client

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type handler struct {
	mu      sync.Mutex
	t       *testing.T
	seenReq []string

	putBody     bytes.Buffer
	getBody     []byte
	headLen     string
	statusByKey map[string]int
	tags        string

	versionListStatus    int
	versionListErrorCode string
	versionListResponses []string
	versionListCalls     int
	deleteObjectsStatus  int
	deleteObjectsErrors  bool
	deleteObjectsCalls   int

	multipartCreateStatus  int
	multipartCreateEmptyID bool
	multipartCreateCalls   int
	uploadPartStatus       int
	uploadPartCalls        int
	completeStatus         int
	completeCalls          int
	abortStatus            int
	abortCalls             int
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seenReq = append(h.seenReq, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery)
	q := r.URL.Query()
	if s, ok := h.statusByKey[r.URL.Path]; ok {
		w.WriteHeader(s)
		return
	}
	switch {
	case r.Method == "GET" && q.Has("versions"):
		h.versionListCalls++
		if h.versionListStatus != 0 {
			w.WriteHeader(h.versionListStatus)
			fmt.Fprintf(w, `<Error><Code>%s</Code><Message>boom</Message></Error>`, h.versionListErrorCode)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		idx := h.versionListCalls - 1
		if idx < len(h.versionListResponses) {
			fmt.Fprint(w, h.versionListResponses[idx])
			return
		}
		fmt.Fprint(w, listVersionsXML(false, "", ""))
	case r.Method == "POST" && q.Has("delete"):
		h.deleteObjectsCalls++
		if h.deleteObjectsStatus != 0 {
			w.WriteHeader(h.deleteObjectsStatus)
			fmt.Fprint(w, `<Error><Code>InternalError</Code><Message>boom</Message></Error>`)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		if h.deleteObjectsErrors {
			fmt.Fprint(w, `<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Error><Key>p/k1</Key><Code>AccessDenied</Code><Message>denied</Message></Error></DeleteResult>`)
			return
		}
		fmt.Fprint(w, `<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`)
	case r.Method == "POST" && q.Has("uploads"):
		h.multipartCreateCalls++
		if h.multipartCreateStatus != 0 {
			w.WriteHeader(h.multipartCreateStatus)
			fmt.Fprint(w, `<Error><Code>InternalError</Code><Message>boom</Message></Error>`)
			return
		}
		uploadID := "upload-1"
		if h.multipartCreateEmptyID {
			uploadID = ""
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>b</Bucket><Key>p/k</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, uploadID)
	case r.Method == "PUT" && q.Has("partNumber") && q.Has("uploadId"):
		h.uploadPartCalls++
		if h.uploadPartStatus != 0 {
			w.WriteHeader(h.uploadPartStatus)
			fmt.Fprint(w, `<Error><Code>InternalError</Code><Message>boom</Message></Error>`)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", `"part-`+q.Get("partNumber")+`"`)
		w.WriteHeader(200)
	case r.Method == "POST" && q.Has("uploadId"):
		h.completeCalls++
		if h.completeStatus != 0 {
			w.WriteHeader(h.completeStatus)
			fmt.Fprint(w, `<Error><Code>InternalError</Code><Message>boom</Message></Error>`)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ETag>"complete"</ETag></CompleteMultipartUploadResult>`)
	case r.Method == "PUT" && q.Has("tagging"):
		// PutObjectTagging
		w.WriteHeader(200)
	case r.Method == "GET" && q.Has("tagging"):
		// GetObjectTagging
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(200)
		fmt.Fprint(w, h.tags)
	case r.Method == "PUT" && r.Header.Get("X-Amz-Copy-Source") != "":
		// CopyObject
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<CopyObjectResult><ETag>"abc"</ETag></CopyObjectResult>`)
	case r.Method == "PUT":
		io.Copy(&h.putBody, r.Body)
		w.Header().Set("ETag", `"abc"`)
		w.WriteHeader(200)
	case r.Method == "GET" && q.Get("list-type") == "2":
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, listXML(q.Get("prefix"), q.Get("delimiter"), q.Get("continuation-token")))
	case r.Method == "GET":
		if r.Header.Get("Range") != "" {
			// range response is just part of getBody
			w.Header().Set("Content-Range", r.Header.Get("Range"))
			w.WriteHeader(206)
		}
		w.Write(h.getBody)
	case r.Method == "HEAD":
		if h.headLen != "" {
			w.Header().Set("Content-Length", h.headLen)
		}
		w.WriteHeader(200)
	case r.Method == "DELETE" && q.Has("uploadId"):
		h.abortCalls++
		if h.abortStatus != 0 {
			w.WriteHeader(h.abortStatus)
			fmt.Fprint(w, `<Error><Code>InternalError</Code><Message>boom</Message></Error>`)
			return
		}
		w.WriteHeader(204)
	case r.Method == "DELETE":
		w.WriteHeader(204)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func listXML(prefix, delimiter, token string) string {
	var b strings.Builder
	b.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	b.WriteString(`<IsTruncated>false</IsTruncated>`)
	b.WriteString(`<NextContinuationToken>tok</NextContinuationToken>`)
	b.WriteString(`<Contents><Key>` + xmlEsc(prefix+"k1") + `</Key></Contents>`)
	if delimiter != "" {
		b.WriteString(`<CommonPrefixes><Prefix>` + xmlEsc(prefix+"sub/") + `</Prefix></CommonPrefixes>`)
	}
	if token != "" {
		b.WriteString(`<Contents><Key>` + xmlEsc(prefix+"k2") + `</Key></Contents>`)
	}
	b.WriteString(`</ListBucketResult>`)
	return b.String()
}

func listVersionsXML(truncated bool, nextKey, nextVersion string) string {
	var b strings.Builder
	b.WriteString(`<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	if truncated {
		b.WriteString(`<IsTruncated>true</IsTruncated>`)
		b.WriteString(`<NextKeyMarker>` + xmlEsc(nextKey) + `</NextKeyMarker>`)
		b.WriteString(`<NextVersionIdMarker>` + xmlEsc(nextVersion) + `</NextVersionIdMarker>`)
	} else {
		b.WriteString(`<IsTruncated>false</IsTruncated>`)
	}
	b.WriteString(`<Version><Key>p/k1</Key><VersionId>v1</VersionId><IsLatest>true</IsLatest></Version>`)
	b.WriteString(`<DeleteMarker><Key>p/k2</Key><VersionId>d1</VersionId><IsLatest>false</IsLatest></DeleteMarker>`)
	b.WriteString(`</ListVersionsResult>`)
	return b.String()
}

func xmlEsc(s string) string {
	var b bytes.Buffer
	xml.EscapeText(&b, []byte(s))
	return b.String()
}

func newAWSTestClient(t *testing.T, h *handler) (*AWS, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	a := NewAWS(AWSConfig{
		Endpoint:             srv.URL,
		Region:               "us-east-1",
		Bucket:               "b",
		Prefix:               "p/",
		PathStyle:            true,
		Credentials:          credentials.NewStaticCredentialsProvider("AK", "SK", ""),
		HTTPClient:           srv.Client(),
		MultipartPartSize:    8 << 20,
		MultipartConcurrency: 2,
	})
	_ = u
	return a, srv
}

func TestAWSPutGet(t *testing.T) {
	h := &handler{t: t, getBody: []byte("hello")}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	ctx := context.Background()
	if err := a.Put(ctx, "k", bytes.NewReader([]byte("hello")), 5); err != nil {
		t.Fatal(err)
	}
	if h.putBody.String() != "hello" {
		t.Fatalf("body=%q", h.putBody.String())
	}
	rc, err := a.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if string(b) != "hello" {
		t.Fatalf("got %q", b)
	}
}

func TestAWSGetError(t *testing.T) {
	h := &handler{t: t, statusByKey: map[string]int{"/b/p/k": 500}}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	if _, err := a.Get(context.Background(), "k"); err == nil {
		t.Fatal("expected error")
	}
}

func TestAWSRangeGet(t *testing.T) {
	h := &handler{t: t, getBody: []byte("rangedata")}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	rc, err := a.RangeGet(context.Background(), "k", 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, rc)
	rc.Close()
}

func TestAWSRangeGetError(t *testing.T) {
	h := &handler{t: t, statusByKey: map[string]int{"/b/p/k": 500}}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	if _, err := a.RangeGet(context.Background(), "k", 0, 1); err == nil {
		t.Fatal("expected error")
	}
}

func TestAWSRangeGetInvalidRange(t *testing.T) {
	h := &handler{t: t}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	for _, tc := range []struct {
		offset int64
		length int64
	}{
		{offset: -1, length: 1},
		{offset: 0, length: 0},
		{offset: 0, length: -1},
	} {
		if _, err := a.RangeGet(context.Background(), "k", tc.offset, tc.length); err == nil {
			t.Fatalf("expected error for offset=%d length=%d", tc.offset, tc.length)
		}
	}
	if len(h.seenReq) != 0 {
		t.Fatalf("invalid range should not issue HTTP request, saw %v", h.seenReq)
	}
}

func TestAWSHead(t *testing.T) {
	h := &handler{t: t, headLen: "42"}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	n, err := a.Head(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if n != 42 {
		t.Fatalf("got %d", n)
	}
}

func TestAWSHeadNoLength(t *testing.T) {
	h := &handler{t: t, headLen: ""}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	n, err := a.Head(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("got %d", n)
	}
}

func TestAWSHeadError(t *testing.T) {
	h := &handler{t: t, statusByKey: map[string]int{"/b/p/k": 500}}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	if _, err := a.Head(context.Background(), "k"); err == nil {
		t.Fatal("expected error")
	}
}

func TestAWSDelete(t *testing.T) {
	h := &handler{t: t}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	if err := a.Delete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
}

func TestAWSDeleteMany(t *testing.T) {
	h := &handler{t: t}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	n, err := a.DeleteMany(context.Background(), []string{"k1", "p/k2"})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("deleted %d", n)
	}
	if h.deleteObjectsCalls != 1 {
		t.Fatalf("delete objects calls=%d", h.deleteObjectsCalls)
	}
	n, err = a.DeleteMany(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("empty delete deleted %d", n)
	}

	keys := make([]string, 1001)
	for i := range keys {
		keys[i] = fmt.Sprintf("bulk-%d", i)
	}
	n, err = a.DeleteMany(context.Background(), keys)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1001 {
		t.Fatalf("bulk deleted %d", n)
	}
	if h.deleteObjectsCalls != 3 {
		t.Fatalf("delete objects calls=%d", h.deleteObjectsCalls)
	}
}

func TestAWSDeleteManyError(t *testing.T) {
	h := &handler{t: t, deleteObjectsErrors: true}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	if _, err := a.DeleteMany(context.Background(), []string{"k"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestAWSDeleteVersions(t *testing.T) {
	h := &handler{t: t, versionListResponses: []string{
		listVersionsXML(true, "p/k2", "d1"),
		listVersionsXML(false, "", ""),
	}}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	n, err := a.DeleteVersions(context.Background(), "sub/")
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("deleted %d", n)
	}
	if h.versionListCalls != 2 || h.deleteObjectsCalls != 2 {
		t.Fatalf("list calls=%d delete calls=%d", h.versionListCalls, h.deleteObjectsCalls)
	}
}

func TestAWSDeleteVersionsListError(t *testing.T) {
	h := &handler{t: t, versionListStatus: 500, versionListErrorCode: "InternalError"}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	if _, err := a.DeleteVersions(context.Background(), "sub/"); err == nil {
		t.Fatal("expected error")
	}
}

func TestAWSDeleteVersionsUnsupported(t *testing.T) {
	h := &handler{t: t, versionListStatus: 405, versionListErrorCode: "MethodNotAllowed"}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	n, err := a.DeleteVersions(context.Background(), "sub/")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("deleted %d", n)
	}
}

func TestAWSDeleteVersionsDeleteError(t *testing.T) {
	h := &handler{t: t, versionListResponses: []string{listVersionsXML(false, "", "")}, deleteObjectsStatus: 500}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	if _, err := a.DeleteVersions(context.Background(), "sub/"); err == nil {
		t.Fatal("expected error")
	}
}

func TestAWSDeleteVersionsEmbeddedDeleteError(t *testing.T) {
	h := &handler{t: t, versionListResponses: []string{listVersionsXML(false, "", "")}, deleteObjectsErrors: true}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	if _, err := a.DeleteVersions(context.Background(), "sub/"); err == nil {
		t.Fatal("expected error")
	}
}

func TestUnsupportedVersionListingPlainError(t *testing.T) {
	if isUnsupportedVersionListing(errors.New("plain")) {
		t.Fatal("plain errors are not unsupported-version-listing responses")
	}
}

func TestAWSList(t *testing.T) {
	h := &handler{t: t}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	res, err := a.List(context.Background(), "sub/", "/", "tok", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Keys) == 0 {
		t.Fatal("no keys returned")
	}
	if len(res.CommonPrefixes) != 1 {
		t.Fatalf("want 1 prefix, got %d", len(res.CommonPrefixes))
	}
	if res.NextContinuation == "" {
		t.Fatal("want continuation")
	}
}

func TestAWSListNoDelimNoToken(t *testing.T) {
	h := &handler{t: t}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	if _, err := a.List(context.Background(), "sub/", "", "", 10); err != nil {
		t.Fatal(err)
	}
}

func TestAWSListError(t *testing.T) {
	h := &handler{t: t, statusByKey: map[string]int{"/b": 500}}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	if _, err := a.List(context.Background(), "", "", "", 10); err == nil {
		t.Fatal("expected error")
	}
}

func TestAWSCopy(t *testing.T) {
	h := &handler{t: t}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	if err := a.Copy(context.Background(), "src", "dst"); err != nil {
		t.Fatal(err)
	}
}

func TestAWSTagging(t *testing.T) {
	h := &handler{t: t, tags: `<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>`}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	tags, err := a.GetObjectTagging(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if tags["a"] != "b" {
		t.Fatalf("got tags %+v", tags)
	}
	if err := a.PutObjectTagging(context.Background(), "k", map[string]string{"x": "y"}); err != nil {
		t.Fatal(err)
	}
}

func TestAWSGetTaggingError(t *testing.T) {
	h := &handler{t: t, statusByKey: map[string]int{"/b/p/k": 500}}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	if _, err := a.GetObjectTagging(context.Background(), "k"); err == nil {
		t.Fatal("expected error")
	}
}

func TestAWSPutObject_Error(t *testing.T) {
	h := &handler{t: t, statusByKey: map[string]int{"/b/p/k": 500}}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	if err := a.Put(context.Background(), "k", bytes.NewReader([]byte("x")), 1); err == nil {
		t.Fatal("expected error")
	}
}

func TestAWSMultipartUpload_SinglePart(t *testing.T) {
	h := &handler{t: t}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	// Bodies at or below the part-size threshold route through PutObject.
	if err := a.MultipartUpload(context.Background(), "k", bytes.NewReader([]byte("hi")), 2); err != nil {
		t.Fatal(err)
	}
	if h.multipartCreateCalls != 0 {
		t.Fatalf("unexpected multipart create calls=%d", h.multipartCreateCalls)
	}
}

func TestAWSMultipartUpload_ReaderAt(t *testing.T) {
	h := &handler{t: t}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	a.partSize = 2
	a.multipartConcurrency = 2
	if err := a.MultipartUpload(context.Background(), "k", bytes.NewReader([]byte("hello")), 5); err != nil {
		t.Fatal(err)
	}
	if h.multipartCreateCalls != 1 || h.uploadPartCalls != 3 || h.completeCalls != 1 || h.abortCalls != 0 {
		t.Fatalf("create=%d parts=%d complete=%d abort=%d", h.multipartCreateCalls, h.uploadPartCalls, h.completeCalls, h.abortCalls)
	}
}

func TestAWSMultipartUpload_ReaderOnly(t *testing.T) {
	h := &handler{t: t}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	a.partSize = 2
	if err := a.MultipartUpload(context.Background(), "k", readOnly{strings.NewReader("hello")}, 5); err != nil {
		t.Fatal(err)
	}
	if h.uploadPartCalls != 3 {
		t.Fatalf("upload parts=%d", h.uploadPartCalls)
	}
}

func TestAWSMultipartUploadInvalidSize(t *testing.T) {
	h := &handler{t: t}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	if err := a.MultipartUpload(context.Background(), "k", bytes.NewReader(nil), -1); err == nil {
		t.Fatal("expected error")
	}
}

func TestAWSMultipartUploadCreateError(t *testing.T) {
	h := &handler{t: t, multipartCreateStatus: 500}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	a.partSize = 2
	if err := a.MultipartUpload(context.Background(), "k", bytes.NewReader([]byte("hello")), 5); err == nil {
		t.Fatal("expected error")
	}
	if h.abortCalls != 0 {
		t.Fatalf("abort calls=%d", h.abortCalls)
	}
}

func TestAWSMultipartUploadEmptyUploadID(t *testing.T) {
	h := &handler{t: t, multipartCreateEmptyID: true}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	a.partSize = 2
	if err := a.MultipartUpload(context.Background(), "k", bytes.NewReader([]byte("hello")), 5); err == nil {
		t.Fatal("expected error")
	}
}

func TestAWSMultipartUploadPartErrorAborts(t *testing.T) {
	h := &handler{t: t, uploadPartStatus: 500}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	a.partSize = 2
	if err := a.MultipartUpload(context.Background(), "k", bytes.NewReader([]byte("hello")), 5); err == nil {
		t.Fatal("expected error")
	}
	if h.abortCalls != 1 {
		t.Fatalf("abort calls=%d", h.abortCalls)
	}
}

func TestAWSMultipartUploadCompleteErrorAborts(t *testing.T) {
	h := &handler{t: t, completeStatus: 500}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	a.partSize = 2
	if err := a.MultipartUpload(context.Background(), "k", bytes.NewReader([]byte("hello")), 5); err == nil {
		t.Fatal("expected error")
	}
	if h.abortCalls != 1 {
		t.Fatalf("abort calls=%d", h.abortCalls)
	}
}

func TestAWSMultipartUploadAbortErrorJoined(t *testing.T) {
	h := &handler{t: t, completeStatus: 500, abortStatus: 500}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	a.partSize = 2
	err := a.MultipartUpload(context.Background(), "k", bytes.NewReader([]byte("hello")), 5)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "abort multipart upload") {
		t.Fatalf("expected joined abort error, got %v", err)
	}
}

func TestAWSMultipartUploadReaderOnlyReadErrorAborts(t *testing.T) {
	h := &handler{t: t}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	a.partSize = 2
	err := a.MultipartUpload(context.Background(), "k", errReader{}, 5)
	if err == nil {
		t.Fatal("expected error")
	}
	if h.abortCalls != 1 {
		t.Fatalf("abort calls=%d", h.abortCalls)
	}
}

func TestAWSMultipartUploadReaderOnlyUploadErrorAborts(t *testing.T) {
	h := &handler{t: t, uploadPartStatus: 500}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	a.partSize = 2
	err := a.MultipartUpload(context.Background(), "k", readOnly{strings.NewReader("hello")}, 5)
	if err == nil {
		t.Fatal("expected error")
	}
	if h.abortCalls != 1 {
		t.Fatalf("abort calls=%d", h.abortCalls)
	}
}

func TestAWSConfigNoEndpoint(t *testing.T) {
	// Just exercise the nil-endpoint branch of NewAWS.
	a := NewAWS(AWSConfig{
		Region:      "us-east-1",
		Bucket:      "b",
		Credentials: credentials.NewStaticCredentialsProvider("AK", "SK", ""),
	})
	if a == nil {
		t.Fatal("nil client")
	}
}

func TestIsNotFound(t *testing.T) {
	if IsNotFound(nil) {
		t.Fatal("nil err -> not found?")
	}
	if IsNotFound(errors.New("plain")) {
		t.Fatal("plain err -> not found?")
	}
	// Typed NoSuchKey error path.
	if !IsNotFound(&types.NoSuchKey{}) {
		t.Fatal("NoSuchKey should be IsNotFound")
	}
	if !IsNotFound(ErrNotFound) {
		t.Fatal("ErrNotFound should be IsNotFound")
	}
	// Force a 404 via httptest + HeadObject (ResponseError path).
	h := &handler{t: t, statusByKey: map[string]int{"/b/p/k": 404}}
	a, srv := newAWSTestClient(t, h)
	defer srv.Close()
	_, err := a.Head(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error")
	}
	if !IsNotFound(err) {
		t.Fatalf("expected IsNotFound, got %v", err)
	}

	// Non-404 HTTP errors should stay false on the ResponseError path.
	h500 := &handler{t: t, statusByKey: map[string]int{"/b/p/k500": 500}}
	a500, srv500 := newAWSTestClient(t, h500)
	defer srv500.Close()
	_, err = a500.Head(context.Background(), "k500")
	if err == nil {
		t.Fatal("expected error")
	}
	if IsNotFound(err) {
		t.Fatalf("unexpected IsNotFound for non-404 error: %v", err)
	}
}

type readOnly struct {
	r *strings.Reader
}

func (r readOnly) Read(p []byte) (int, error) { return r.r.Read(p) }

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("boom") }

// ensure aws import used in all paths
var _ = aws.String
