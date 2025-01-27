// Copyright (C) 2014 Nippon Telegraph and Telephone Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/osrg/gobgp/config"
	"github.com/osrg/gobgp/packet"
	"github.com/osrg/gobgp/table"
	"gopkg.in/tomb.v2"
	"io"
	"math/rand"
	"net"
	"strconv"
	"time"
)

type FsmStateReason int

const (
	_ FsmStateReason = iota
	FSM_DYING
	FSM_ADMIN_DOWN
	FSM_READ_FAILED
	FSM_WRITE_FAILED
	FSM_NOTIFICATION_SENT
	FSM_NOTIFICATION_RECV
	FSM_HOLD_TIMER_EXPIRED
	FSM_IDLE_HOLD_TIMER_EXPIRED
	FSM_RESTART_TIMER_EXPIRED
	FSM_GRACEFUL_RESTART
	FSM_INVALID_MSG
)

func (r FsmStateReason) String() string {
	switch r {
	case FSM_DYING:
		return "dying"
	case FSM_ADMIN_DOWN:
		return "admin-down"
	case FSM_READ_FAILED:
		return "read-failed"
	case FSM_WRITE_FAILED:
		return "write-failed"
	case FSM_NOTIFICATION_SENT:
		return "notification-sent"
	case FSM_NOTIFICATION_RECV:
		return "notification-recved"
	case FSM_HOLD_TIMER_EXPIRED:
		return "hold-timer-expired"
	case FSM_IDLE_HOLD_TIMER_EXPIRED:
		return "idle-hold-timer-expired"
	case FSM_RESTART_TIMER_EXPIRED:
		return "restart-timer-expired"
	case FSM_GRACEFUL_RESTART:
		return "graceful-restart"
	case FSM_INVALID_MSG:
		return "invalid-msg"
	}
	return "unknown"
}

type FsmMsgType int

const (
	_ FsmMsgType = iota
	FSM_MSG_STATE_CHANGE
	FSM_MSG_BGP_MESSAGE
)

type FsmMsg struct {
	MsgType   FsmMsgType
	MsgSrc    string
	MsgDst    string
	MsgData   interface{}
	PathList  []*table.Path
	timestamp time.Time
	payload   []byte
}

const (
	HOLDTIME_OPENSENT = 240
	HOLDTIME_IDLE     = 5
)

type AdminState int

const (
	ADMIN_STATE_UP AdminState = iota
	ADMIN_STATE_DOWN
)

func (s AdminState) String() string {
	switch s {
	case ADMIN_STATE_UP:
		return "ADMIN_STATE_UP"
	case ADMIN_STATE_DOWN:
		return "ADMIN_STATE_DOWN"
	default:
		return "Unknown"
	}
}

type FSM struct {
	t                tomb.Tomb
	gConf            *config.Global
	pConf            *config.Neighbor
	state            bgp.FSMState
	reason           FsmStateReason
	conn             net.Conn
	connCh           chan net.Conn
	idleHoldTime     float64
	opensentHoldTime float64
	adminState       AdminState
	adminStateCh     chan AdminState
	getActiveCh      chan struct{}
	h                *FSMHandler
	rfMap            map[bgp.RouteFamily]bool
	capMap           map[bgp.BGPCapabilityCode][]bgp.ParameterCapabilityInterface
	recvOpen         *bgp.BGPMessage
	peerInfo         *table.PeerInfo
	policy           *table.RoutingPolicy
}

func (fsm *FSM) bgpMessageStateUpdate(MessageType uint8, isIn bool) {
	state := &fsm.pConf.State.Messages
	timer := &fsm.pConf.Timers
	if isIn {
		state.Received.Total++
	} else {
		state.Sent.Total++
	}
	switch MessageType {
	case bgp.BGP_MSG_OPEN:
		if isIn {
			state.Received.Open++
		} else {
			state.Sent.Open++
		}
	case bgp.BGP_MSG_UPDATE:
		if isIn {
			state.Received.Update++
			timer.State.UpdateRecvTime = time.Now().Unix()
		} else {
			state.Sent.Update++
		}
	case bgp.BGP_MSG_NOTIFICATION:
		if isIn {
			state.Received.Notification++
		} else {
			state.Sent.Notification++
		}
	case bgp.BGP_MSG_KEEPALIVE:
		if isIn {
			state.Received.Keepalive++
		} else {
			state.Sent.Keepalive++
		}
	case bgp.BGP_MSG_ROUTE_REFRESH:
		if isIn {
			state.Received.Refresh++
		} else {
			state.Sent.Refresh++
		}
	default:
		if isIn {
			state.Received.Discarded++
		} else {
			state.Sent.Discarded++
		}
	}
}

func NewFSM(gConf *config.Global, pConf *config.Neighbor, policy *table.RoutingPolicy) *FSM {
	adminState := ADMIN_STATE_UP
	if pConf.State.AdminDown {
		adminState = ADMIN_STATE_DOWN
	}
	fsm := &FSM{
		gConf:            gConf,
		pConf:            pConf,
		state:            bgp.BGP_FSM_IDLE,
		connCh:           make(chan net.Conn, 1),
		opensentHoldTime: float64(HOLDTIME_OPENSENT),
		adminState:       adminState,
		adminStateCh:     make(chan AdminState, 1),
		getActiveCh:      make(chan struct{}),
		rfMap:            make(map[bgp.RouteFamily]bool),
		capMap:           make(map[bgp.BGPCapabilityCode][]bgp.ParameterCapabilityInterface),
		peerInfo:         table.NewPeerInfo(gConf, pConf),
		policy:           policy,
	}
	fsm.t.Go(fsm.connectLoop)
	return fsm
}

func (fsm *FSM) StateChange(nextState bgp.FSMState) {
	log.WithFields(log.Fields{
		"Topic":  "Peer",
		"Key":    fsm.pConf.Config.NeighborAddress,
		"old":    fsm.state.String(),
		"new":    nextState.String(),
		"reason": fsm.reason.String(),
	}).Debug("state changed")
	fsm.state = nextState
	switch nextState {
	case bgp.BGP_FSM_ESTABLISHED:
		fsm.pConf.Timers.State.Uptime = time.Now().Unix()
		fsm.pConf.State.EstablishedCount++
	case bgp.BGP_FSM_ACTIVE:
		if !fsm.pConf.Transport.Config.PassiveMode {
			fsm.getActiveCh <- struct{}{}
		}
		fallthrough
	default:
		fsm.pConf.Timers.State.Downtime = time.Now().Unix()
	}
}

func hostport(addr net.Addr) (string, uint16) {
	if addr != nil {
		host, port, err := net.SplitHostPort(addr.String())
		if err != nil {
			return "", 0
		}
		p, _ := strconv.Atoi(port)
		return host, uint16(p)
	}
	return "", 0
}

func (fsm *FSM) RemoteHostPort() (string, uint16) {
	return hostport(fsm.conn.RemoteAddr())

}

func (fsm *FSM) LocalHostPort() (string, uint16) {
	return hostport(fsm.conn.LocalAddr())
}

func (fsm *FSM) sendNotificatonFromErrorMsg(conn net.Conn, e *bgp.MessageError) {
	m := bgp.NewBGPNotificationMessage(e.TypeCode, e.SubTypeCode, e.Data)
	b, _ := m.Serialize()
	_, err := conn.Write(b)
	if err != nil {
		fsm.bgpMessageStateUpdate(m.Header.Type, false)
	}
	conn.Close()

	log.WithFields(log.Fields{
		"Topic": "Peer",
		"Key":   fsm.pConf.Config.NeighborAddress,
		"Data":  e,
	}).Warn("sent notification")
}

func (fsm *FSM) sendNotification(conn net.Conn, code, subType uint8, data []byte, msg string) {
	e := bgp.NewMessageError(code, subType, data, msg)
	fsm.sendNotificatonFromErrorMsg(conn, e.(*bgp.MessageError))
}

func (fsm *FSM) connectLoop() error {
	var tick int
	if tick = int(fsm.pConf.Timers.Config.ConnectRetry); tick < MIN_CONNECT_RETRY {
		tick = MIN_CONNECT_RETRY
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	ticker := time.NewTicker(time.Duration(tick) * time.Second)
	ticker.Stop()

	connect := func() {
		if fsm.state == bgp.BGP_FSM_ACTIVE {
			addr := fsm.pConf.Config.NeighborAddress
			host := net.JoinHostPort(addr, strconv.Itoa(bgp.BGP_PORT))
			// check if LocalAddress has been configured
			laddr := fsm.pConf.Transport.Config.LocalAddress
			if laddr != "" {
				lhost := net.JoinHostPort(laddr, "0")
				ltcpaddr, err := net.ResolveTCPAddr("tcp", lhost)
				if err != nil {
					log.WithFields(log.Fields{
						"Topic": "Peer",
						"Key":   fsm.pConf.Config.NeighborAddress,
					}).Warnf("failed to resolve ltcpaddr: %s", err)
				} else {
					d := net.Dialer{LocalAddr: ltcpaddr, Timeout: time.Duration(MIN_CONNECT_RETRY-1) * time.Second}
					if conn, err := d.Dial("tcp", host); err == nil {
						fsm.connCh <- conn
					} else {
						log.WithFields(log.Fields{
							"Topic": "Peer",
							"Key":   fsm.pConf.Config.NeighborAddress,
						}).Debugf("failed to connect from ltcpaddr", err)
					}
				}

			} else {
				conn, err := net.DialTimeout("tcp", host, time.Duration(MIN_CONNECT_RETRY-1)*time.Second)
				if err == nil {
					fsm.connCh <- conn
				} else {
					log.WithFields(log.Fields{
						"Topic": "Peer",
						"Key":   fsm.pConf.Config.NeighborAddress,
					}).Debugf("failed to connect: %s", err)
				}
			}
		}
	}

	for {
		select {
		case <-fsm.t.Dying():
			log.WithFields(log.Fields{
				"Topic": "Peer",
				"Key":   fsm.pConf.Config.NeighborAddress,
			}).Debug("stop connect loop")
			ticker.Stop()
			return nil
		case <-ticker.C:
			connect()
		case <-fsm.getActiveCh:
			time.Sleep(time.Duration(r.Intn(MIN_CONNECT_RETRY)+MIN_CONNECT_RETRY) * time.Second)
			connect()
			ticker = time.NewTicker(time.Duration(tick) * time.Second)
		}
	}
}

type FSMHandler struct {
	t                tomb.Tomb
	fsm              *FSM
	conn             net.Conn
	msgCh            chan *FsmMsg
	errorCh          chan FsmStateReason
	incoming         chan *FsmMsg
	stateCh          chan *FsmMsg
	outgoing         chan *bgp.BGPMessage
	holdTimerResetCh chan bool
}

func NewFSMHandler(fsm *FSM, incoming, stateCh chan *FsmMsg, outgoing chan *bgp.BGPMessage) *FSMHandler {
	h := &FSMHandler{
		fsm:              fsm,
		errorCh:          make(chan FsmStateReason, 2),
		incoming:         incoming,
		stateCh:          stateCh,
		outgoing:         outgoing,
		holdTimerResetCh: make(chan bool, 2),
	}
	fsm.t.Go(h.loop)
	return h
}

func (h *FSMHandler) idle() (bgp.FSMState, FsmStateReason) {
	fsm := h.fsm

	idleHoldTimer := time.NewTimer(time.Second * time.Duration(fsm.idleHoldTime))
	for {
		select {
		case <-h.t.Dying():
			return -1, FSM_DYING
		case conn, ok := <-fsm.connCh:
			if !ok {
				break
			}
			conn.Close()
			log.WithFields(log.Fields{
				"Topic": "Peer",
				"Key":   fsm.pConf.Config.NeighborAddress,
			}).Warn("Closed an accepted connection")
		case <-idleHoldTimer.C:

			if fsm.adminState == ADMIN_STATE_UP {
				log.WithFields(log.Fields{
					"Topic":    "Peer",
					"Key":      fsm.pConf.Config.NeighborAddress,
					"Duration": fsm.idleHoldTime,
				}).Debug("IdleHoldTimer expired")
				fsm.idleHoldTime = HOLDTIME_IDLE
				return bgp.BGP_FSM_ACTIVE, FSM_IDLE_HOLD_TIMER_EXPIRED

			} else {
				log.Debug("IdleHoldTimer expired, but stay at idle because the admin state is DOWN")
			}

		case s := <-fsm.adminStateCh:
			err := h.changeAdminState(s)
			if err == nil {
				switch s {
				case ADMIN_STATE_DOWN:
					// stop idle hold timer
					idleHoldTimer.Stop()

				case ADMIN_STATE_UP:
					// restart idle hold timer
					idleHoldTimer.Reset(time.Second * time.Duration(fsm.idleHoldTime))
				}
			}
		}
	}
}

func (h *FSMHandler) active() (bgp.FSMState, FsmStateReason) {
	fsm := h.fsm
	for {
		select {
		case <-h.t.Dying():
			return -1, FSM_DYING
		case conn, ok := <-fsm.connCh:
			if !ok {
				break
			}
			fsm.conn = conn
			if fsm.gConf.Config.As != fsm.pConf.Config.PeerAs {
				ttl := 1
				if fsm.pConf.EbgpMultihop.Config.Enabled == true {
					ttl = int(fsm.pConf.EbgpMultihop.Config.MultihopTtl)
				}
				if ttl != 0 {
					SetTcpTTLSockopts(conn.(*net.TCPConn), ttl)
				}
			}
			// we don't implement delayed open timer so move to opensent right
			// away.
			return bgp.BGP_FSM_OPENSENT, 0
		case err := <-h.errorCh:
			return bgp.BGP_FSM_IDLE, err
		case s := <-fsm.adminStateCh:
			err := h.changeAdminState(s)
			if err == nil {
				switch s {
				case ADMIN_STATE_DOWN:
					return bgp.BGP_FSM_IDLE, FSM_ADMIN_DOWN
				case ADMIN_STATE_UP:
					log.WithFields(log.Fields{
						"Topic":      "Peer",
						"Key":        fsm.pConf.Config.NeighborAddress,
						"State":      fsm.state,
						"AdminState": s.String(),
					}).Panic("code logic bug")
				}
			}
		}
	}
}

func capabilitiesFromConfig(gConf *config.Global, pConf *config.Neighbor) []bgp.ParameterCapabilityInterface {
	caps := make([]bgp.ParameterCapabilityInterface, 0, 4)
	caps = append(caps, bgp.NewCapRouteRefresh())
	for _, rf := range pConf.AfiSafis {
		family, _ := bgp.GetRouteFamily(string(rf.AfiSafiName))
		caps = append(caps, bgp.NewCapMultiProtocol(family))
	}
	caps = append(caps, bgp.NewCapFourOctetASNumber(gConf.Config.As))
	return caps
}

func buildopen(gConf *config.Global, pConf *config.Neighbor) *bgp.BGPMessage {
	caps := capabilitiesFromConfig(gConf, pConf)
	opt := bgp.NewOptionParameterCapability(caps)
	holdTime := uint16(pConf.Timers.Config.HoldTime)
	as := gConf.Config.As
	if as > (1<<16)-1 {
		as = bgp.AS_TRANS
	}
	return bgp.NewBGPOpenMessage(uint16(as), holdTime, gConf.Config.RouterId,
		[]bgp.OptionParameterInterface{opt})
}

func readAll(conn net.Conn, length int) ([]byte, error) {
	buf := make([]byte, length)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (h *FSMHandler) recvMessageWithError() error {
	headerBuf, err := readAll(h.conn, bgp.BGP_HEADER_LENGTH)
	if err != nil {
		h.errorCh <- FSM_READ_FAILED
		return err
	}

	hd := &bgp.BGPHeader{}
	err = hd.DecodeFromBytes(headerBuf)
	if err != nil {
		h.fsm.bgpMessageStateUpdate(0, true)
		log.WithFields(log.Fields{
			"Topic": "Peer",
			"Key":   h.fsm.pConf.Config.NeighborAddress,
			"State": h.fsm.state,
			"error": err,
		}).Warn("malformed BGP Header")
		h.msgCh <- &FsmMsg{
			MsgType: FSM_MSG_BGP_MESSAGE,
			MsgSrc:  h.fsm.pConf.Config.NeighborAddress,
			MsgDst:  h.fsm.pConf.Transport.Config.LocalAddress,
			MsgData: err,
		}
		return err
	}

	bodyBuf, err := readAll(h.conn, int(hd.Len)-bgp.BGP_HEADER_LENGTH)
	if err != nil {
		h.errorCh <- FSM_READ_FAILED
		return err
	}

	now := time.Now()
	m, err := bgp.ParseBGPBody(hd, bodyBuf)
	if err == nil {
		h.fsm.bgpMessageStateUpdate(m.Header.Type, true)
		err = bgp.ValidateBGPMessage(m)
	} else {
		h.fsm.bgpMessageStateUpdate(0, true)
	}
	fmsg := &FsmMsg{
		MsgType:   FSM_MSG_BGP_MESSAGE,
		MsgSrc:    h.fsm.pConf.Config.NeighborAddress,
		MsgDst:    h.fsm.pConf.Transport.Config.LocalAddress,
		timestamp: now,
	}
	if err != nil {
		log.WithFields(log.Fields{
			"Topic": "Peer",
			"Key":   h.fsm.pConf.Config.NeighborAddress,
			"State": h.fsm.state,
			"error": err,
		}).Warn("malformed BGP message")
		fmsg.MsgData = err
	} else {
		fmsg.MsgData = m
		if h.fsm.state == bgp.BGP_FSM_ESTABLISHED {
			switch m.Header.Type {
			case bgp.BGP_MSG_UPDATE:
				body := m.Body.(*bgp.BGPUpdate)
				confedCheck := !config.IsConfederationMember(h.fsm.gConf, h.fsm.pConf) && config.IsEBGPPeer(h.fsm.gConf, h.fsm.pConf)
				_, err := bgp.ValidateUpdateMsg(body, h.fsm.rfMap, confedCheck)
				if err != nil {
					log.WithFields(log.Fields{
						"Topic": "Peer",
						"Key":   h.fsm.pConf.Config.NeighborAddress,
						"error": err,
					}).Warn("malformed BGP update message")
					fmsg.MsgData = err
				} else {
					// FIXME: we should use the original message for bmp/mrt
					table.UpdatePathAttrs4ByteAs(body)
					fmsg.PathList = table.ProcessMessage(m, h.fsm.peerInfo, fmsg.timestamp)
					id := h.fsm.pConf.Config.NeighborAddress
					policyMutex.RLock()
					for _, path := range fmsg.PathList {
						if h.fsm.policy.ApplyPolicy(id, table.POLICY_DIRECTION_IN, path, nil) == nil {
							path.Filter(id, table.POLICY_DIRECTION_IN)
						}
					}
					policyMutex.RUnlock()
				}
				fmsg.payload = make([]byte, len(headerBuf)+len(bodyBuf))
				copy(fmsg.payload, headerBuf)
				copy(fmsg.payload[len(headerBuf):], bodyBuf)
				fallthrough
			case bgp.BGP_MSG_KEEPALIVE:
				// if the lenght of h.holdTimerResetCh
				// isn't zero, the timer will be reset
				// soon anyway.
				if len(h.holdTimerResetCh) == 0 {
					h.holdTimerResetCh <- true
				}
				if m.Header.Type == bgp.BGP_MSG_KEEPALIVE {
					return nil
				}
			case bgp.BGP_MSG_NOTIFICATION:
				body := m.Body.(*bgp.BGPNotification)
				log.WithFields(log.Fields{
					"Topic":   "Peer",
					"Key":     h.fsm.pConf.Config.NeighborAddress,
					"Code":    body.ErrorCode,
					"Subcode": body.ErrorSubcode,
					"Data":    body.Data,
				}).Warn("received notification")
				h.errorCh <- FSM_NOTIFICATION_RECV
				return nil
			}
		}
	}
	h.msgCh <- fmsg
	return err
}

func (h *FSMHandler) recvMessage() error {
	h.recvMessageWithError()
	return nil
}

func open2Cap(open *bgp.BGPOpen, n *config.Neighbor) (map[bgp.BGPCapabilityCode][]bgp.ParameterCapabilityInterface, map[bgp.RouteFamily]bool) {
	capMap := make(map[bgp.BGPCapabilityCode][]bgp.ParameterCapabilityInterface)
	rfMap := config.CreateRfMap(n)
	r := make(map[bgp.RouteFamily]bool)
	for _, p := range open.OptParams {
		if paramCap, y := p.(*bgp.OptionParameterCapability); y {
			for _, c := range paramCap.Capability {
				m, ok := capMap[c.Code()]
				if !ok {
					m = make([]bgp.ParameterCapabilityInterface, 0, 1)
				}
				capMap[c.Code()] = append(m, c)

				if c.Code() == bgp.BGP_CAP_MULTIPROTOCOL {
					m := c.(*bgp.CapMultiProtocol)
					r[m.CapValue] = true
				}
			}
		}
	}

	if len(r) > 0 {
		for rf, _ := range rfMap {
			if _, y := r[rf]; !y {
				delete(rfMap, rf)
			}
		}
	} else {
		rfMap = make(map[bgp.RouteFamily]bool)
		rfMap[bgp.RF_IPv4_UC] = true
	}
	return capMap, rfMap
}

func (h *FSMHandler) opensent() (bgp.FSMState, FsmStateReason) {
	fsm := h.fsm
	m := buildopen(fsm.gConf, fsm.pConf)
	b, _ := m.Serialize()
	fsm.conn.Write(b)
	fsm.bgpMessageStateUpdate(m.Header.Type, false)

	h.msgCh = make(chan *FsmMsg)
	h.conn = fsm.conn

	h.t.Go(h.recvMessage)

	// RFC 4271 P.60
	// sets its HoldTimer to a large value
	// A HoldTimer value of 4 minutes is suggested as a "large value"
	// for the HoldTimer
	holdTimer := time.NewTimer(time.Second * time.Duration(fsm.opensentHoldTime))

	for {
		select {
		case <-h.t.Dying():
			h.conn.Close()
			return -1, FSM_DYING
		case conn, ok := <-fsm.connCh:
			if !ok {
				break
			}
			conn.Close()
			log.WithFields(log.Fields{
				"Topic": "Peer",
				"Key":   fsm.pConf.Config.NeighborAddress,
				"State": fsm.state,
			}).Warn("Closed an accepted connection")
		case e := <-h.msgCh:
			switch e.MsgData.(type) {
			case *bgp.BGPMessage:
				m := e.MsgData.(*bgp.BGPMessage)
				if m.Header.Type == bgp.BGP_MSG_OPEN {
					fsm.recvOpen = m
					body := m.Body.(*bgp.BGPOpen)
					err := bgp.ValidateOpenMsg(body, fsm.pConf.Config.PeerAs)
					if err != nil {
						fsm.sendNotificatonFromErrorMsg(h.conn, err.(*bgp.MessageError))
						return bgp.BGP_FSM_IDLE, FSM_INVALID_MSG
					}
					fsm.peerInfo.ID = body.ID
					fsm.capMap, fsm.rfMap = open2Cap(body, fsm.pConf)

					// calculate HoldTime
					// RFC 4271 P.13
					// a BGP speaker MUST calculate the value of the Hold Timer
					// by using the smaller of its configured Hold Time and the Hold Time
					// received in the OPEN message.
					holdTime := float64(body.HoldTime)
					myHoldTime := fsm.pConf.Timers.Config.HoldTime
					if holdTime > myHoldTime {
						fsm.pConf.Timers.State.NegotiatedHoldTime = myHoldTime
					} else {
						fsm.pConf.Timers.State.NegotiatedHoldTime = holdTime
					}

					keepalive := fsm.pConf.Timers.Config.KeepaliveInterval
					if n := fsm.pConf.Timers.State.NegotiatedHoldTime; n < myHoldTime {
						keepalive = n / 3
					}
					fsm.pConf.Timers.State.KeepaliveInterval = keepalive

					msg := bgp.NewBGPKeepAliveMessage()
					b, _ := msg.Serialize()
					fsm.conn.Write(b)
					fsm.bgpMessageStateUpdate(msg.Header.Type, false)
					return bgp.BGP_FSM_OPENCONFIRM, 0
				} else {
					// send notification?
					h.conn.Close()
					return bgp.BGP_FSM_IDLE, FSM_INVALID_MSG
				}
			case *bgp.MessageError:
				fsm.sendNotificatonFromErrorMsg(h.conn, e.MsgData.(*bgp.MessageError))
				return bgp.BGP_FSM_IDLE, FSM_INVALID_MSG
			default:
				log.WithFields(log.Fields{
					"Topic": "Peer",
					"Key":   fsm.pConf.Config.NeighborAddress,
					"State": fsm.state,
					"Data":  e.MsgData,
				}).Panic("unknown msg type")
			}
		case err := <-h.errorCh:
			h.conn.Close()
			return bgp.BGP_FSM_IDLE, err
		case <-holdTimer.C:
			fsm.sendNotification(h.conn, bgp.BGP_ERROR_HOLD_TIMER_EXPIRED, 0, nil, "hold timer expired")
			h.t.Kill(nil)
			return bgp.BGP_FSM_IDLE, FSM_HOLD_TIMER_EXPIRED
		case s := <-fsm.adminStateCh:
			err := h.changeAdminState(s)
			if err == nil {
				switch s {
				case ADMIN_STATE_DOWN:
					h.conn.Close()
					return bgp.BGP_FSM_IDLE, FSM_ADMIN_DOWN
				case ADMIN_STATE_UP:
					log.WithFields(log.Fields{
						"Topic":      "Peer",
						"Key":        fsm.pConf.Config.NeighborAddress,
						"State":      fsm.state,
						"AdminState": s.String(),
					}).Panic("code logic bug")
				}
			}
		}
	}
}

func keepaliveTicker(fsm *FSM) *time.Ticker {
	negotiatedTime := fsm.pConf.Timers.State.NegotiatedHoldTime
	if negotiatedTime == 0 {
		return &time.Ticker{}
	}
	sec := time.Second * time.Duration(fsm.pConf.Timers.State.KeepaliveInterval)
	if sec == 0 {
		sec = 1
	}
	return time.NewTicker(sec)
}

func (h *FSMHandler) openconfirm() (bgp.FSMState, FsmStateReason) {
	fsm := h.fsm
	ticker := keepaliveTicker(fsm)
	h.msgCh = make(chan *FsmMsg)
	h.conn = fsm.conn

	h.t.Go(h.recvMessage)

	var holdTimer *time.Timer
	if fsm.pConf.Timers.State.NegotiatedHoldTime == 0 {
		holdTimer = &time.Timer{}
	} else {
		// RFC 4271 P.65
		// sets the HoldTimer according to the negotiated value
		holdTimer = time.NewTimer(time.Second * time.Duration(fsm.pConf.Timers.State.NegotiatedHoldTime))
	}

	for {
		select {
		case <-h.t.Dying():
			h.conn.Close()
			return -1, FSM_DYING
		case conn, ok := <-fsm.connCh:
			if !ok {
				break
			}
			conn.Close()
			log.WithFields(log.Fields{
				"Topic": "Peer",
				"Key":   fsm.pConf.Config.NeighborAddress,
				"State": fsm.state,
			}).Warn("Closed an accepted connection")
		case <-ticker.C:
			m := bgp.NewBGPKeepAliveMessage()
			b, _ := m.Serialize()
			// TODO: check error
			fsm.conn.Write(b)
			fsm.bgpMessageStateUpdate(m.Header.Type, false)
		case e := <-h.msgCh:
			switch e.MsgData.(type) {
			case *bgp.BGPMessage:
				m := e.MsgData.(*bgp.BGPMessage)
				nextState := bgp.BGP_FSM_IDLE
				if m.Header.Type == bgp.BGP_MSG_KEEPALIVE {
					nextState = bgp.BGP_FSM_ESTABLISHED
				} else {
					// send notification ?
					h.conn.Close()
				}
				return nextState, 0
			case *bgp.MessageError:
				fsm.sendNotificatonFromErrorMsg(h.conn, e.MsgData.(*bgp.MessageError))
				return bgp.BGP_FSM_IDLE, FSM_INVALID_MSG
			default:
				log.WithFields(log.Fields{
					"Topic": "Peer",
					"Key":   fsm.pConf.Config.NeighborAddress,
					"State": fsm.state,
					"Data":  e.MsgData,
				}).Panic("unknown msg type")
			}
		case err := <-h.errorCh:
			h.conn.Close()
			return bgp.BGP_FSM_IDLE, err
		case <-holdTimer.C:
			fsm.sendNotification(h.conn, bgp.BGP_ERROR_HOLD_TIMER_EXPIRED, 0, nil, "hold timer expired")
			h.t.Kill(nil)
			return bgp.BGP_FSM_IDLE, FSM_HOLD_TIMER_EXPIRED
		case s := <-fsm.adminStateCh:
			err := h.changeAdminState(s)
			if err == nil {
				switch s {
				case ADMIN_STATE_DOWN:
					h.conn.Close()
					return bgp.BGP_FSM_IDLE, FSM_ADMIN_DOWN
				case ADMIN_STATE_UP:
					log.WithFields(log.Fields{
						"Topic":      "Peer",
						"Key":        fsm.pConf.Config.NeighborAddress,
						"State":      fsm.state,
						"AdminState": s.String(),
					}).Panic("code logic bug")
				}
			}
		}
	}
}

func (h *FSMHandler) sendMessageloop() error {
	conn := h.conn
	fsm := h.fsm
	ticker := keepaliveTicker(fsm)
	send := func(m *bgp.BGPMessage) error {
		b, err := m.Serialize()
		if err != nil {
			log.WithFields(log.Fields{
				"Topic": "Peer",
				"Key":   fsm.pConf.Config.NeighborAddress,
				"State": fsm.state,
				"Data":  err,
			}).Warn("failed to serialize")
			fsm.bgpMessageStateUpdate(0, false)
			return nil
		}
		if err := conn.SetWriteDeadline(time.Now().Add(time.Second * time.Duration(fsm.pConf.Timers.State.NegotiatedHoldTime))); err != nil {
			h.errorCh <- FSM_WRITE_FAILED
			return fmt.Errorf("failed to set write deadline")
		}
		_, err = conn.Write(b)
		if err != nil {
			log.WithFields(log.Fields{
				"Topic": "Peer",
				"Key":   fsm.pConf.Config.NeighborAddress,
				"State": fsm.state,
				"Data":  err,
			}).Warn("failed to send")
			h.errorCh <- FSM_WRITE_FAILED
			return fmt.Errorf("closed")
		}
		fsm.bgpMessageStateUpdate(m.Header.Type, false)

		if m.Header.Type == bgp.BGP_MSG_NOTIFICATION {
			log.WithFields(log.Fields{
				"Topic": "Peer",
				"Key":   fsm.pConf.Config.NeighborAddress,
				"State": fsm.state,
				"Data":  m,
			}).Warn("sent notification")
			h.errorCh <- FSM_NOTIFICATION_SENT
			return fmt.Errorf("closed")
		} else {
			log.WithFields(log.Fields{
				"Topic": "Peer",
				"Key":   fsm.pConf.Config.NeighborAddress,
				"State": fsm.state,
				"data":  m,
			}).Debug("sent")
		}
		return nil
	}

	for {
		select {
		case <-h.t.Dying():
			// a) if a configuration is deleted, we need
			// to send notification before we die.
			//
			// b) if a recv goroutin found that the
			// connection is closed and tried to kill us,
			// we need to die immediately. Otherwise fms
			// doesn't go to idle.
			//
			// we always try to send. in case b), the
			// connection was already closed so it
			// correctly works in both cases.
			if h.fsm.state == bgp.BGP_FSM_ESTABLISHED {
				send(bgp.NewBGPNotificationMessage(bgp.BGP_ERROR_CEASE, bgp.BGP_ERROR_SUB_PEER_DECONFIGURED, nil))
			}
			return nil
		case m := <-h.outgoing:
			if err := send(m); err != nil {
				return nil
			}
		case <-ticker.C:
			if err := send(bgp.NewBGPKeepAliveMessage()); err != nil {
				return nil
			}

		}
	}
}

func (h *FSMHandler) recvMessageloop() error {
	for {
		err := h.recvMessageWithError()
		if err != nil {
			return nil
		}
	}
}

func (h *FSMHandler) established() (bgp.FSMState, FsmStateReason) {
	fsm := h.fsm
	h.conn = fsm.conn
	h.t.Go(h.sendMessageloop)
	h.msgCh = h.incoming
	h.t.Go(h.recvMessageloop)

	var holdTimer *time.Timer
	if fsm.pConf.Timers.State.NegotiatedHoldTime == 0 {
		holdTimer = &time.Timer{}
	} else {
		holdTimer = time.NewTimer(time.Second * time.Duration(fsm.pConf.Timers.State.NegotiatedHoldTime))
	}

	for {
		select {
		case <-h.t.Dying():
			return -1, FSM_DYING
		case conn, ok := <-fsm.connCh:
			if !ok {
				break
			}
			conn.Close()
			log.WithFields(log.Fields{
				"Topic": "Peer",
				"Key":   fsm.pConf.Config.NeighborAddress,
				"State": fsm.state,
			}).Warn("Closed an accepted connection")
		case err := <-h.errorCh:
			h.conn.Close()
			h.t.Kill(nil)
			return bgp.BGP_FSM_IDLE, err
		case <-holdTimer.C:
			log.WithFields(log.Fields{
				"Topic": "Peer",
				"Key":   fsm.pConf.Config.NeighborAddress,
				"State": fsm.state,
				"data":  bgp.BGP_FSM_ESTABLISHED,
			}).Warn("hold timer expired")
			m := bgp.NewBGPNotificationMessage(bgp.BGP_ERROR_HOLD_TIMER_EXPIRED, 0, nil)
			h.outgoing <- m
			return bgp.BGP_FSM_IDLE, FSM_HOLD_TIMER_EXPIRED
		case <-h.holdTimerResetCh:
			if fsm.pConf.Timers.State.NegotiatedHoldTime != 0 {
				holdTimer.Reset(time.Second * time.Duration(fsm.pConf.Timers.State.NegotiatedHoldTime))
			}
		case s := <-fsm.adminStateCh:
			err := h.changeAdminState(s)
			if err == nil {
				switch s {
				case ADMIN_STATE_DOWN:
					m := bgp.NewBGPNotificationMessage(
						bgp.BGP_ERROR_CEASE, bgp.BGP_ERROR_SUB_ADMINISTRATIVE_SHUTDOWN, nil)
					h.outgoing <- m
				}
			}
		}
	}
}

func (h *FSMHandler) loop() error {
	fsm := h.fsm
	ch := make(chan bgp.FSMState)
	oldState := fsm.state

	f := func() error {
		nextState := bgp.FSMState(-1)
		var reason FsmStateReason
		switch fsm.state {
		case bgp.BGP_FSM_IDLE:
			nextState, reason = h.idle()
			// case bgp.BGP_FSM_CONNECT:
			// 	nextState = h.connect()
		case bgp.BGP_FSM_ACTIVE:
			nextState, reason = h.active()
		case bgp.BGP_FSM_OPENSENT:
			nextState, reason = h.opensent()
		case bgp.BGP_FSM_OPENCONFIRM:
			nextState, reason = h.openconfirm()
		case bgp.BGP_FSM_ESTABLISHED:
			nextState, reason = h.established()
		}
		fsm.reason = reason
		ch <- nextState
		return nil
	}

	h.t.Go(f)

	nextState := <-ch

	if nextState == bgp.BGP_FSM_ESTABLISHED && oldState == bgp.BGP_FSM_OPENCONFIRM {
		log.WithFields(log.Fields{
			"Topic": "Peer",
			"Key":   fsm.pConf.Config.NeighborAddress,
			"State": fsm.state,
		}).Info("Peer Up")
	}

	if oldState == bgp.BGP_FSM_ESTABLISHED {
		log.WithFields(log.Fields{
			"Topic":  "Peer",
			"Key":    fsm.pConf.Config.NeighborAddress,
			"State":  fsm.state,
			"Reason": fsm.reason,
		}).Info("Peer Down")
	}

	e := time.AfterFunc(time.Second*120, func() {
		log.Fatal("failed to free the fsm.h.t for ", fsm.pConf.Config.NeighborAddress, oldState, nextState)
	})
	h.t.Wait()
	e.Stop()

	// under zero means that tomb.Dying()
	if nextState >= bgp.BGP_FSM_IDLE {
		e := &FsmMsg{
			MsgType: FSM_MSG_STATE_CHANGE,
			MsgSrc:  fsm.pConf.Config.NeighborAddress,
			MsgDst:  fsm.pConf.Transport.Config.LocalAddress,
			MsgData: nextState,
		}
		h.stateCh <- e
	}
	return nil
}

func (h *FSMHandler) changeAdminState(s AdminState) error {
	fsm := h.fsm
	if fsm.adminState != s {
		log.WithFields(log.Fields{
			"Topic":      "Peer",
			"Key":        fsm.pConf.Config.NeighborAddress,
			"State":      fsm.state,
			"AdminState": s.String(),
		}).Debug("admin state changed")

		fsm.adminState = s

		switch s {
		case ADMIN_STATE_UP:
			log.WithFields(log.Fields{
				"Topic": "Peer",
				"Key":   fsm.pConf.Config.NeighborAddress,
				"State": fsm.state,
			}).Info("Administrative start")

		case ADMIN_STATE_DOWN:
			log.WithFields(log.Fields{
				"Topic": "Peer",
				"Key":   fsm.pConf.Config.NeighborAddress,
				"State": fsm.state,
			}).Info("Administrative shutdown")
		}

	} else {
		log.WithFields(log.Fields{
			"Topic": "Peer",
			"Key":   fsm.pConf.Config.NeighborAddress,
			"State": fsm.state,
		}).Warn("cannot change to the same state")

		return fmt.Errorf("cannot change to the same state.")
	}
	return nil
}
