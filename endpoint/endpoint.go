// Package endpoint implements replication endpoints for use with package replication.
package endpoint

import (
	"bytes"
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/problame/go-streamrpc"
	"github.com/zrepl/zrepl/replication"
	"github.com/zrepl/zrepl/replication/pdu"
	"github.com/zrepl/zrepl/zfs"
	"io"
)

// Sender implements replication.ReplicationEndpoint for a sending side
type Sender struct {
	FSFilter                zfs.DatasetFilter
	FilesystemVersionFilter zfs.FilesystemVersionFilter
}

func NewSender(fsf zfs.DatasetFilter, fsvf zfs.FilesystemVersionFilter) *Sender {
	return &Sender{fsf, fsvf}
}

func (p *Sender) ListFilesystems(ctx context.Context) ([]*pdu.Filesystem, error) {
	fss, err := zfs.ZFSListMapping(p.FSFilter)
	if err != nil {
		return nil, err
	}
	rfss := make([]*pdu.Filesystem, len(fss))
	for i := range fss {
		rfss[i] = &pdu.Filesystem{
			Path: fss[i].ToString(),
			// FIXME: not supporting ResumeToken yet
		}
	}
	return rfss, nil
}

func (p *Sender) ListFilesystemVersions(ctx context.Context, fs string) ([]*pdu.FilesystemVersion, error) {
	dp, err := zfs.NewDatasetPath(fs)
	if err != nil {
		return nil, err
	}
	pass, err := p.FSFilter.Filter(dp)
	if err != nil {
		return nil, err
	}
	if !pass {
		return nil, replication.NewFilteredError(fs)
	}
	fsvs, err := zfs.ZFSListFilesystemVersions(dp, p.FilesystemVersionFilter)
	if err != nil {
		return nil, err
	}
	rfsvs := make([]*pdu.FilesystemVersion, len(fsvs))
	for i := range fsvs {
		rfsvs[i] = pdu.FilesystemVersionFromZFS(&fsvs[i])
	}
	return rfsvs, nil
}

func (p *Sender) Send(ctx context.Context, r *pdu.SendReq) (*pdu.SendRes, io.ReadCloser, error) {
	dp, err := zfs.NewDatasetPath(r.Filesystem)
	if err != nil {
		return nil, nil, err
	}
	pass, err := p.FSFilter.Filter(dp)
	if err != nil {
		return nil, nil, err
	}
	if !pass {
		return nil, nil, replication.NewFilteredError(r.Filesystem)
	}

	size, err := zfs.ZFSSendDry(r.Filesystem, r.From, r.To)
	if err != nil {
		return nil, nil, err
	}

	stream, err := zfs.ZFSSend(r.Filesystem, r.From, r.To)
	if err != nil {
		return nil, nil, err
	}
	return &pdu.SendRes{ExpectedSize: size}, stream, nil
}

func (p *Sender) DestroySnapshots(ctx context.Context, req *pdu.DestroySnapshotsReq) (*pdu.DestroySnapshotsRes, error) {
	dp, err := zfs.NewDatasetPath(req.Filesystem)
	if err != nil {
		return nil, err
	}
	pass, err := p.FSFilter.Filter(dp)
	if err != nil {
		return nil, err
	}
	if !pass {
		return nil, replication.NewFilteredError(req.Filesystem)
	}

	return doDestroySnapshots(ctx, dp, req.Snapshots)
}

// Since replication always happens from sender to receiver, this method  is only ipmlemented for the sender.
// If this method returns a *zfs.DatasetDoesNotExist as an error, it might be a good indicator
// that something is wrong with the pruning logic, which is the only consumer of this method.
func (p *Sender) SnapshotReplicationStatus(ctx context.Context, req *pdu.SnapshotReplicationStatusReq) (*pdu.SnapshotReplicationStatusRes, error) {
	dp, err := zfs.NewDatasetPath(req.Filesystem)
	if err != nil {
		return nil, err
	}
	pass, err := p.FSFilter.Filter(dp)
	if err != nil {
		return nil, err
	}
	if !pass {
		return nil, replication.NewFilteredError(req.Filesystem)
	}

	version := zfs.FilesystemVersion{
		Type: zfs.Snapshot,
		Name: req.Snapshot, //FIXME validation
	}

	replicated := false
	switch req.Op {
	case pdu.SnapshotReplicationStatusReq_Get:
		replicated, err = zfs.ZFSGetReplicatedProperty(dp, &version)
		if err != nil {
			return nil, err
		}
	case pdu.SnapshotReplicationStatusReq_SetReplicated:
		err = zfs.ZFSSetReplicatedProperty(dp, &version, true)
		if err != nil {
			return nil, err
		}
		replicated = true
	default:
		return nil, errors.Errorf("unknown opcode %v", req.Op)
	}
	return &pdu.SnapshotReplicationStatusRes{Replicated: replicated}, nil
}

type FSFilter interface {
	Filter(path *zfs.DatasetPath) (pass bool, err error)
}

// FIXME: can we get away without error types here?
type FSMap interface {
	FSFilter
	Map(path *zfs.DatasetPath) (*zfs.DatasetPath, error)
	Invert() (FSMap, error)
	AsFilter() FSFilter
}

// Receiver implements replication.ReplicationEndpoint for a receiving side
type Receiver struct {
	fsmapInv FSMap
	fsmap    FSMap
	fsvf     zfs.FilesystemVersionFilter
}

func NewReceiver(fsmap FSMap, fsvf zfs.FilesystemVersionFilter) (*Receiver, error) {
	fsmapInv, err := fsmap.Invert()
	if err != nil {
		return nil, err
	}
	return &Receiver{fsmapInv, fsmap, fsvf}, nil
}

func (e *Receiver) ListFilesystems(ctx context.Context) ([]*pdu.Filesystem, error) {
	filtered, err := zfs.ZFSListMapping(e.fsmapInv.AsFilter())
	if err != nil {
		return nil, errors.Wrap(err, "error checking client permission")
	}
	fss := make([]*pdu.Filesystem, len(filtered))
	for i, a := range filtered {
		mapped, err := e.fsmapInv.Map(a)
		if err != nil {
			return nil, err
		}
		fss[i] = &pdu.Filesystem{Path: mapped.ToString()}
	}
	return fss, nil
}

func (e *Receiver) ListFilesystemVersions(ctx context.Context, fs string) ([]*pdu.FilesystemVersion, error) {
	p, err := zfs.NewDatasetPath(fs)
	if err != nil {
		return nil, err
	}
	lp, err := e.fsmap.Map(p)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return nil, errors.New("access to filesystem denied")
	}

	fsvs, err := zfs.ZFSListFilesystemVersions(lp, e.fsvf)
	if err != nil {
		return nil, err
	}

	rfsvs := make([]*pdu.FilesystemVersion, len(fsvs))
	for i := range fsvs {
		rfsvs[i] = pdu.FilesystemVersionFromZFS(&fsvs[i])
	}

	return rfsvs, nil
}

func (e *Receiver) Receive(ctx context.Context, req *pdu.ReceiveReq, sendStream io.ReadCloser) error {
	defer sendStream.Close()

	p, err := zfs.NewDatasetPath(req.Filesystem)
	if err != nil {
		return err
	}
	lp, err := e.fsmap.Map(p)
	if err != nil {
		return err
	}
	if lp == nil {
		return errors.New("receive to filesystem denied")
	}

	// create placeholder parent filesystems as appropriate
	var visitErr error
	f := zfs.NewDatasetPathForest()
	f.Add(lp)
	getLogger(ctx).Debug("begin tree-walk")
	f.WalkTopDown(func(v zfs.DatasetPathVisit) (visitChildTree bool) {
		if v.Path.Equal(lp) {
			return false
		}
		_, err := zfs.ZFSGet(v.Path, []string{zfs.ZREPL_PLACEHOLDER_PROPERTY_NAME})
		if err != nil {
			// interpret this as an early exit of the zfs binary due to the fs not existing
			if err := zfs.ZFSCreatePlaceholderFilesystem(v.Path); err != nil {
				getLogger(ctx).
					WithError(err).
					WithField("placeholder_fs", v.Path).
					Error("cannot create placeholder filesystem")
				visitErr = err
				return false
			}
		}
		getLogger(ctx).WithField("filesystem", v.Path.ToString()).Debug("exists")
		return true // leave this fs as is
	})
	getLogger(ctx).WithField("visitErr", visitErr).Debug("complete tree-walk")

	if visitErr != nil {
		return visitErr
	}

	needForceRecv := false
	props, err := zfs.ZFSGet(lp, []string{zfs.ZREPL_PLACEHOLDER_PROPERTY_NAME})
	if err == nil {
		if isPlaceholder, _ := zfs.IsPlaceholder(lp, props.Get(zfs.ZREPL_PLACEHOLDER_PROPERTY_NAME)); isPlaceholder {
			needForceRecv = true
		}
	}

	args := make([]string, 0, 1)
	if needForceRecv {
		args = append(args, "-F")
	}

	getLogger(ctx).Debug("start receive command")

	if err := zfs.ZFSRecv(lp.ToString(), sendStream, args...); err != nil {
		return err
	}
	return nil
}

func (e *Receiver) DestroySnapshots(ctx context.Context, req *pdu.DestroySnapshotsReq) (*pdu.DestroySnapshotsRes, error) {
	dp, err := zfs.NewDatasetPath(req.Filesystem)
	if err != nil {
		return nil, err
	}
	lp, err := e.fsmap.Map(dp)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return nil, errors.New("access to filesystem denied")
	}
	return doDestroySnapshots(ctx, lp, req.Snapshots)
}

func doDestroySnapshots(ctx context.Context, lp *zfs.DatasetPath, snaps []*pdu.FilesystemVersion) (*pdu.DestroySnapshotsRes, error) {
	fsvs := make([]*zfs.FilesystemVersion, len(snaps))
	for i, fsv := range snaps {
		if fsv.Type != pdu.FilesystemVersion_Snapshot {
			return nil, fmt.Errorf("version %q is not a snapshot", fsv.Name)
		}
		var err error
		fsvs[i], err = fsv.ZFSFilesystemVersion()
		if err != nil {
			return nil, err
		}
	}
	res := &pdu.DestroySnapshotsRes{
		Results: make([]*pdu.DestroySnapshotRes, len(fsvs)),
	}
	for i, fsv := range fsvs {
		err := zfs.ZFSDestroyFilesystemVersion(lp, fsv)
		errMsg := ""
		if err != nil {
			errMsg = err.Error()
		}
		res.Results[i] = &pdu.DestroySnapshotRes{
			Snapshot: pdu.FilesystemVersionFromZFS(fsv),
			Error:    errMsg,
		}
	}
	return res, nil
}

// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
// RPC STUBS
// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

const (
	RPCListFilesystems           = "ListFilesystems"
	RPCListFilesystemVersions    = "ListFilesystemVersions"
	RPCReceive                   = "Receive"
	RPCSend                      = "Send"
	RPCSDestroySnapshots         = "DestroySnapshots"
	RPCSnapshotReplicationStatus = "SnapshotReplicationStatus"
)

// Remote implements an endpoint stub that uses streamrpc as a transport.
type Remote struct {
	c *streamrpc.Client
}

func NewRemote(c *streamrpc.Client) Remote {
	return Remote{c}
}

func (s Remote) ListFilesystems(ctx context.Context) ([]*pdu.Filesystem, error) {
	req := pdu.ListFilesystemReq{}
	b, err := proto.Marshal(&req)
	if err != nil {
		return nil, err
	}
	rb, rs, err := s.c.RequestReply(ctx, RPCListFilesystems, bytes.NewBuffer(b), nil)
	if err != nil {
		return nil, err
	}
	if rs != nil {
		rs.Close()
		return nil, errors.New("response contains unexpected stream")
	}
	var res pdu.ListFilesystemRes
	if err := proto.Unmarshal(rb.Bytes(), &res); err != nil {
		return nil, err
	}
	return res.Filesystems, nil
}

func (s Remote) ListFilesystemVersions(ctx context.Context, fs string) ([]*pdu.FilesystemVersion, error) {
	req := pdu.ListFilesystemVersionsReq{
		Filesystem: fs,
	}
	b, err := proto.Marshal(&req)
	if err != nil {
		return nil, err
	}
	rb, rs, err := s.c.RequestReply(ctx, RPCListFilesystemVersions, bytes.NewBuffer(b), nil)
	if err != nil {
		return nil, err
	}
	if rs != nil {
		rs.Close()
		return nil, errors.New("response contains unexpected stream")
	}
	var res pdu.ListFilesystemVersionsRes
	if err := proto.Unmarshal(rb.Bytes(), &res); err != nil {
		return nil, err
	}
	return res.Versions, nil
}

func (s Remote) Send(ctx context.Context, r *pdu.SendReq) (*pdu.SendRes, io.ReadCloser, error) {
	b, err := proto.Marshal(r)
	if err != nil {
		return nil, nil, err
	}
	rb, rs, err := s.c.RequestReply(ctx, RPCSend, bytes.NewBuffer(b), nil)
	if err != nil {
		return nil, nil, err
	}
	if rs == nil {
		return nil, nil, errors.New("response does not contain a stream")
	}
	var res pdu.SendRes
	if err := proto.Unmarshal(rb.Bytes(), &res); err != nil {
		rs.Close()
		return nil, nil, err
	}
	return &res, rs, nil
}

func (s Remote) Receive(ctx context.Context, r *pdu.ReceiveReq, sendStream io.ReadCloser) error {
	defer sendStream.Close()
	b, err := proto.Marshal(r)
	if err != nil {
		return err
	}
	rb, rs, err := s.c.RequestReply(ctx, RPCReceive, bytes.NewBuffer(b), sendStream)
	if err != nil {
		return err
	}
	if rs != nil {
		rs.Close()
		return errors.New("response contains unexpected stream")
	}
	var res pdu.ReceiveRes
	if err := proto.Unmarshal(rb.Bytes(), &res); err != nil {
		return err
	}
	return nil
}

func (s Remote) DestroySnapshots(ctx context.Context, r *pdu.DestroySnapshotsReq) (*pdu.DestroySnapshotsRes, error) {
	b, err := proto.Marshal(r)
	if err != nil {
		return nil, err
	}
	rb, rs, err := s.c.RequestReply(ctx, RPCSDestroySnapshots, bytes.NewBuffer(b), nil)
	if err != nil {
		return nil, err
	}
	if rs != nil {
		rs.Close()
		return nil, errors.New("response contains unexpected stream")
	}
	var res pdu.DestroySnapshotsRes
	if err := proto.Unmarshal(rb.Bytes(), &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// Handler implements the server-side streamrpc.HandlerFunc for a Remote endpoint stub.
type Handler struct {
	ep replication.Endpoint
}

func NewHandler(ep replication.Endpoint) Handler {
	return Handler{ep}
}

func (a *Handler) Handle(ctx context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {

	switch endpoint {
	case RPCListFilesystems:
		var req pdu.ListFilesystemReq
		if err := proto.Unmarshal(reqStructured.Bytes(), &req); err != nil {
			return nil, nil, err
		}
		fsses, err := a.ep.ListFilesystems(ctx)
		if err != nil {
			return nil, nil, err
		}
		res := &pdu.ListFilesystemRes{
			Filesystems: fsses,
		}
		b, err := proto.Marshal(res)
		if err != nil {
			return nil, nil, err
		}
		return bytes.NewBuffer(b), nil, nil

	case RPCListFilesystemVersions:

		var req pdu.ListFilesystemVersionsReq
		if err := proto.Unmarshal(reqStructured.Bytes(), &req); err != nil {
			return nil, nil, err
		}
		fsvs, err := a.ep.ListFilesystemVersions(ctx, req.Filesystem)
		if err != nil {
			return nil, nil, err
		}
		res := &pdu.ListFilesystemVersionsRes{
			Versions: fsvs,
		}
		b, err := proto.Marshal(res)
		if err != nil {
			return nil, nil, err
		}
		return bytes.NewBuffer(b), nil, nil

	case RPCSend:

		sender, ok := a.ep.(replication.Sender)
		if !ok {
			goto Err
		}

		var req pdu.SendReq
		if err := proto.Unmarshal(reqStructured.Bytes(), &req); err != nil {
			return nil, nil, err
		}
		res, sendStream, err := sender.Send(ctx, &req)
		if err != nil {
			return nil, nil, err
		}
		b, err := proto.Marshal(res)
		if err != nil {
			return nil, nil, err
		}
		return bytes.NewBuffer(b), sendStream, err

	case RPCReceive:

		receiver, ok := a.ep.(replication.Receiver)
		if !ok {
			goto Err
		}

		var req pdu.ReceiveReq
		if err := proto.Unmarshal(reqStructured.Bytes(), &req); err != nil {
			return nil, nil, err
		}
		err := receiver.Receive(ctx, &req, reqStream)
		if err != nil {
			return nil, nil, err
		}
		b, err := proto.Marshal(&pdu.ReceiveRes{})
		if err != nil {
			return nil, nil, err
		}
		return bytes.NewBuffer(b), nil, err

	case RPCSDestroySnapshots:

		var req pdu.DestroySnapshotsReq
		if err := proto.Unmarshal(reqStructured.Bytes(), &req); err != nil {
			return nil, nil, err
		}

		res, err := a.ep.DestroySnapshots(ctx, &req)
		if err != nil {
			return nil, nil, err
		}
		b, err := proto.Marshal(res)
		if err != nil {
			return nil, nil, err
		}
		return bytes.NewBuffer(b), nil, nil

	case RPCSnapshotReplicationStatus:

		sender, ok := a.ep.(replication.Sender)
		if !ok {
			goto Err
		}

		var req pdu.SnapshotReplicationStatusReq
		if err := proto.Unmarshal(reqStructured.Bytes(), &req); err != nil {
			return nil, nil, err
		}
		res, err := sender.SnapshotReplicationStatus(ctx, &req)
		if err != nil {
			return nil, nil, err
		}
		b, err := proto.Marshal(res)
		if err != nil {
			return nil, nil, err
		}
		return bytes.NewBuffer(b), nil, nil

	}
Err:
	return nil, nil, errors.New("no handler for given endpoint")
}
